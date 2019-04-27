import argparse
import json
import msgpack
import time
import os
from collections import defaultdict
from collections import Counter
import string
import numpy as np
import hashlib
import logging
import psutil
import subprocess

import ray
from ray.tests.cluster_utils import Cluster
import ray.cloudpickle as pickle
from ray.experimental import named_actors

from cython_examples import cython_process_batch4 as cython_process_batch_map
from cython_examples import process_batch_reducer3 as cython_process_batch_reducer


CHECKPOINT_DIR = '/tmp/ray-checkpoints'
SENTENCE_LENGTH = 100

LOG_LEVEL = logging.INFO

WORDS = {
        }


def wait_queue(logger, queue, max_queue_length):
    if len(queue) <= max_queue_length:
        return

    # Check pending downstream tasks. Update queue in place.
    _, queue[:] = ray.wait(
            queue,
            num_returns=len(queue),
            timeout=0)

    wait_time = 0
    while len(queue) > max_queue_length:
        _, queue[:] = ray.wait(queue, len(queue), timeout=0.1, request_once=True)
        wait_time += 0.1
        logger.debug("length of queue is now %d", len(queue))

        # Hack to resubmit the last task. If we've waited for a while and
        # there's still no progress, then try a long-standing ray.wait on
        # the last task that we submitted to resubmit it.
        if wait_time > 0.3 and len(queue) > 0:
            _, queue[:] = ray.wait(queue[:], 1, timeout=0)
            wait_time = 0
            logger.debug("XXX length of queue is now %d", len(queue))

def backpressured_push(logger, handle, queue, num_tasks, max_queue_length, args, nondeterministic_event=None):
    num_return_vals = 0
    if num_tasks % max_queue_length  == 0:
        num_return_vals = 1
    obj_id = handle.push._remote(
            args=args,
            kwargs={},
            num_return_vals=num_return_vals,
            nondeterministic_event=nondeterministic_event)
    if obj_id:
        queue.append(obj_id)
    return obj_id


# A custom data source that reads articles from wikipedia
# Custom data sources need to implement a get_next() method
# that returns the next data element, in this case sentences
@ray.remote(max_reconstructions=100)
class WordSource(object):
    def __init__(self,
            operator_index,
            operator_id,
            handles,
            max_queue_length,
            checkpoint_dir,
            checkpoint_interval,
            words_file,
            timestamp_interval,
            backpressure,
            cpu_indices):
        p = psutil.Process()
        p.cpu_affinity(cpu_indices)

        logging.basicConfig(level=LOG_LEVEL)
        self.logger = logging.getLogger(__name__)

        with open(words_file) as f:
            self.words = []
            for line in f.readlines():
                self.words.append(line.strip())
        self.words = np.array([word.encode('ascii') for word in self.words])
        self.timestamp_interval = timestamp_interval

        self.operator_index = operator_index
        self.operator_id = operator_id
        self.handles = handles
        self.queue = []
        self.max_queue_length = max_queue_length
        self.backpressure = backpressure

        # How many checkpoints have been taken so far.
        self.checkpoint_epoch = 0
        # Number of records in an epoch.
        self.checkpoint_interval = checkpoint_interval
        self.records_since_checkpoint = 0
        self.num_records_seen = 0
        self.num_records_since_timestamp = 0
        self.num_flushes = 0
        self.record_timestamp = None

        # Create the checkpoint directory.
        self.checkpoint_dir = checkpoint_dir
        try:
            os.makedirs(self.checkpoint_dir)
        except FileExistsError:
            pass

        self.checkpoint_attrs = [
                "handles",
                "checkpoint_epoch",
                "records_since_checkpoint",
                "num_records_seen",
                "num_records_since_timestamp",
                "num_flushes",
                "record_timestamp",
                ]
        if ray.worker.global_worker.task_context.nondeterministic_events is not None:
            self.load_checkpoint()

        # Set the seed so that we can deterministically generate the sentences.
        np.random.seed(self.checkpoint_epoch)

        self.logger.info("SOURCE: %s %s", self.operator_id, ray.worker.global_worker.task_context.nondeterministic_events)

    def save_checkpoint(self):
        with ray.profiling.profile("save_checkpoint"):
            self.logger.debug("Saving checkpoint %d", self.checkpoint_epoch)

            checkpoint = {
                    attr: getattr(self, attr) for attr in self.checkpoint_attrs
                    }
            checkpoint["put_index"] = ray.worker.global_worker.task_context.put_index
            checkpoint = pickle.dumps(checkpoint)
            self.logger.debug("Source checkpoint size is %d", len(checkpoint))
            # NOTE: The default behavior is to register a random actor handle
            # whenever a handle is pickled, so that the execution dependency is
            # never removed and anytime the handle is unpickled, we will be able to
            # submit tasks.  However, we do not need to do this since we are only
            # going to unpickle the handle once, when the actor recovers from the
            # checkpoint.
            [handle._ray_new_actor_handles.clear() for handle in self.handles]

            actor_id = ray.worker.global_worker.actor_id
            checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), self.checkpoint_epoch)
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
            with open(checkpoint_path, 'wb+') as f:
                f.write(checkpoint)

            self.checkpoint_epoch += 1
            # Set the seed so that we can deterministically generate the sentences.
            np.random.seed(self.checkpoint_epoch)
            self.push_checkpoint_marker()

    def load_checkpoint(self):
        with ray.profiling.profile("load_checkpoint"):
            checkpoint_tracker = named_actors.get_actor("checkpoint_tracker")
            obj = checkpoint_tracker.get_current_epoch.remote()
            latest_checkpoint_interval = ray.get(obj)
            self.logger.info("SOURCE Reloading checkpoint %d", latest_checkpoint_interval)
            if latest_checkpoint_interval < 0:
                return False
            # Read the latest checkpoint from disk.
            actor_id = ray.worker.global_worker.actor_id
            checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), latest_checkpoint_interval)
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
            with open(checkpoint_path, 'rb') as f:
                checkpoint = pickle.loads(f.read())
            ray.worker.global_worker.task_context.put_index = checkpoint.pop("put_index")
            for attr, value in checkpoint.items():
                setattr(self, attr, value)
                self.logger.info("Setting %s %s", attr, value)
            [handle.reset_handle_id() for handle in self.handles]

            self.checkpoint_epoch += 1
            self.push_checkpoint_marker()
            return True

    def push_checkpoint_marker(self):
        checkpoint_marker_args = [self.operator_id, 0, [None], self.checkpoint_epoch]
        self.logger.debug("Pushing checkpoint marker %d", self.checkpoint_epoch)
        for handle in self.handles:
            handle.push._remote(
                    args=checkpoint_marker_args,
                    kwargs={},
                    num_return_vals=0)

    def get_batch(self, batch_size):
        #return np.apply_along_axis(lambda line: (b'0', np.string_.join(b' ', line)), 1, np.random.choice(self.words, (batch_size, SENTENCE_LENGTH)))
        return [np.string_.join(b' ', np.random.choice(self.words, SENTENCE_LENGTH)) for _ in range(batch_size)]

    def generate(self, num_records, batch_size, target_throughput=-1):
        handle = self.handles[self.num_flushes % len(self.handles)]

        if target_throughput > -1:
            time_slice = batch_size / target_throughput

        start_time = time.time()
        if self.record_timestamp is None:
            self.record_timestamp = start_time
        while self.num_records_seen < num_records:
            start = time.time()
            batch = self.get_batch(batch_size)
            assert(len(batch) > 0), len(batch)
            timestamp = 0
            if self.num_records_since_timestamp > self.timestamp_interval:
                timestamp = self.record_timestamp + (time_slice * np.random.rand())
                self.num_records_since_timestamp -= self.timestamp_interval
            batch_id = ray.put(batch)

            if target_throughput > -1:
                self.record_timestamp += time_slice
            else:
                duration = time.time() - start
                self.record_timestamp += duration

            remaining = self.record_timestamp - time.time()
            self.logger.debug("REMAINING: %f", remaining)
            if target_throughput == -1 or remaining > 0:
                # We are faster than the downstream actor, so wait for it to
                # catch up.
                wait_queue(self.logger, self.queue, 1)
                remaining = self.record_timestamp - time.time()
                if remaining > 0.001:
                    self.logger.debug("Sleeping for %f, time slice %d", remaining, time_slice)
                    time.sleep(remaining)
                backpressure = True
            else:
                # We're falling beihnd. This is because we are replaying the source.
                backpressure = False

            args = [self.operator_id, timestamp, batch_id, self.checkpoint_epoch]
            self.logger.debug("Pushing record timestamp %f", self.record_timestamp)
            if self.backpressure and backpressure:
                backpressured_push(self.logger, handle, self.queue, self.num_flushes, self.max_queue_length, args)
            else:
                handle.push._remote(
                        args=args,
                        kwargs={},
                        num_return_vals=0)

            self.num_records_seen += len(batch)
            self.num_records_since_timestamp += len(batch)
            self.num_flushes += 1
            handle = self.handles[self.num_flushes % len(self.handles)]

            # Save a checkpoint if we have passed the checkpoint interval.
            self.records_since_checkpoint += len(batch)
            if self.records_since_checkpoint >= self.checkpoint_interval:
                self.records_since_checkpoint -= self.checkpoint_interval
                self.save_checkpoint()

        done = [handle.push.remote(self.operator_id, 0, [], self.checkpoint_epoch) for handle in self.handles]
        self.logger.debug("Waiting for done objects %s", done)
        ray.get(done)
        throughput = self.num_records_seen / (time.time() - start_time)
        return throughput

    def ping(self):
        return


class NondeterministicOperator(ray.actor.Checkpointable):

    def __init__(self, operator_index, operator_id, handles, max_queue_length, upstream_ids, checkpoint_dir, batch_size, submit_batch_size, cpu_indices):
        p = psutil.Process()
        p.cpu_affinity(cpu_indices)

        logging.basicConfig(level=LOG_LEVEL)
        self.logger = logging.getLogger(__name__)

        self.operator_index = operator_index
        self.operator_id = operator_id
        self.handles = handles
        self._ray_downstream_actors = [handle._ray_actor_id for handle in handles]
        self.num_records_seen = 0

        self.upstream_ids = upstream_ids
        self.checkpoints_pending = set()
        self.checkpoint_epoch = 0
        self._should_checkpoint = False

        # Create the checkpoint directory.
        self.checkpoint_dir = checkpoint_dir
        try:
            os.makedirs(self.checkpoint_dir)
        except FileExistsError:
            pass

        self.batch_size = batch_size
        self.flush_buffers = [list() for _ in range(len(handles))]
        self.num_handles = len(handles)
        self.submit_batch_size = submit_batch_size
        # TODO: Make sure this is flushed before a checkpoint.
        self.task_buffer = []

        self.queues = [list() for _ in range(len(handles))]
        self.max_queue_length = max_queue_length

        self.state = None
        self.num_flushes = list(range(len(handles)))
        self.num_flushes = self.num_flushes[self.operator_index:] + self.num_flushes[:self.operator_index]
        self.num_total_flushes = 0
        self.logger.debug("Operator %s starts with queue offsets %s", self.operator_id, self.num_flushes)

        self.checkpoint_attrs = [
                "self_handle",
                "handles",
                "checkpoint_epoch",
                "num_records_seen",
                "flush_buffers",
                "num_flushes",
                "num_total_flushes",
                "_ray_upstream_actor_handle_ids",
                ]

    def register_self_handle(self, self_handle):
        self.self_handle = self_handle

    def register_upstream_actor_handle_ids(self, upstream_actor_handle_ids):
        self._ray_upstream_actor_handle_ids = upstream_actor_handle_ids

    def push(self, upstream_id, timestamp, records, checkpoint_epoch):
        self.logger.debug("PUSH in task %s, upstream_id: %s, num records: %d, checkpoint %d", ray.worker.global_worker.current_task_id.hex(), upstream_id, self.num_records_seen, checkpoint_epoch)
        if ray.worker.global_worker.task_context.nondeterministic_events is not None:
            submit_log = [int(event.decode('ascii')) for event in ray.worker.global_worker.task_context.nondeterministic_events]
            self.logger.debug("REPLAY: Submit log %s", submit_log)
        else:
            submit_log = None

        if submit_log is not None:
            self.replay_push(upstream_id, timestamp, records, checkpoint_epoch, submit_log)
        else:
            self.log_push(upstream_id, timestamp, records, checkpoint_epoch)

    def replay_push(self, upstream_id, timestamp, records, checkpoint_epoch, submit_log):
        self.logger.debug("REPLAY: %s records %d", upstream_id, len(records))
        if len(records) == 1 and records[0] is None:
            self.logger.debug("PUSH: received checkpoint marker %s %d", upstream_id, checkpoint_epoch)
            self.handle_checkpoint_marker(upstream_id, checkpoint_epoch)
            if not self._should_checkpoint:
                # Tell raylet that we haven't finished this task, so that tasks
                # from the same handle are blocked.
                ray.worker.global_worker.notify_task_unfinished()
            return

        if len(records) == 0:
            # This is the last batch that we will receive from this
            # upstream operator.
            for i, flush_buffer in enumerate(self.flush_buffers):
                if len(flush_buffer) > 0:
                    self.flush(i, 0)
            # Send an empty batch. Block on the result to notify the
            # upstream operator when we are finished processing all of its
            # records.
            ray.get([self.flush(i, 0) for i in range(len(self.flush_buffers))])
        else:
            batch_size = self.batch_size
            if self.batch_size is None:
                batch_size = len(records)

            has_state = self.state is not None
            # If the operator doesn't have state and we previously flushed
            # records from this batch, then try to skip those records.
            while not has_state and (submit_log or len(records) >= batch_size):
                executed = True
                for handle in self.handles:
                    next_task_id = ray._raylet.generate_actor_task_id(
                            ray.worker.global_worker.task_driver_id,
                            handle._ray_actor_id,
                            handle._ray_actor_handle_id,
                            handle._ray_actor_counter)
                    task = ray.global_state.task_table(task_id=next_task_id)
                    if not task or task["ExecutionSpec"]["NumExecutions"] < 1:
                        self.logger.debug("REPLAY: never executed task %s", next_task_id)
                        executed = False
                        break
                if not executed:
                    break

                num_skip_records = batch_size
                if submit_log and submit_log[0] - self.num_records_seen < batch_size:
                    num_skip_records = submit_log[0] - self.num_records_seen
                self.logger.debug("REPLAY: skipping: %d, seen: %d, num records: %d", num_skip_records, self.num_records_seen, len(records))
                assert num_skip_records > 0, (num_skip_records, submit_log, self.num_records_seen)


                num_records = len(records)
                records = records[num_skip_records:]
                num_skipped = num_records - len(records)
                self.num_records_seen += num_skipped

                # If initially, we flushed mid-batch or flushed because of
                # a checkpoint, do the same now.
                do_flush = (self.num_records_seen % batch_size == 0)
                was_nondeterministic_flush = len(submit_log) > 0 and submit_log[0] == self.num_records_seen
                do_flush = do_flush or was_nondeterministic_flush
                if do_flush:
                    # Replay an empty flush.
                    for i in range(self.num_handles):
                        #self.flush_buffers[key] = flush_buffer
                        key = (i + self.operator_index) % self.num_handles
                        self.backpressured_flush(key, timestamp, [])
                        self.logger.debug("REPLAY: skipping flush after %d", self.num_records_seen)
                    # We replayed a nondeterministic flush. Pop it from the log.
                    if was_nondeterministic_flush:
                        submit_log.pop(0)

            assert len(submit_log) == 0, "TODO: fix nondeterministic replay"
            for i in range(0, len(records), batch_size):
                batch = records[i * batch_size : (i+1) * batch_size]
                processed_batch = self.process_batch(timestamp, batch)
                self.num_records_seen += len(batch)

                #for key, flush_buffer in enumerate(processed_batch):
                for i in range(len(processed_batch)):
                    #self.flush_buffers[key] = flush_buffer
                    key = (i + self.operator_index) % self.num_handles
                    flush_buffer = processed_batch[key]
                    self.backpressured_flush(key, timestamp, flush_buffer)
                #if was_nondeterministic_flush:
                #    submit_log.pop(0)

    def handle_checkpoint_marker(self, upstream_id, checkpoint_epoch):
        assert not self._should_checkpoint
        if checkpoint_epoch > self.checkpoint_epoch:
            # This is the first checkpoint marker for the new checkpoint
            # interval that we've received so far.
            if len(self.checkpoints_pending) == 0:
                self.logger.debug("Starting checkpoint %d", self.checkpoint_epoch)
                self.checkpoints_pending = set(self.upstream_ids)
            # Record the checkpoint marker received from this upstream actor's
            # operator_id.
            self.logger.debug("Received checkpoint marker %d from %s", checkpoint_epoch, upstream_id)
            self.checkpoints_pending.discard(upstream_id)
            # If we've received all checkpoint markers from all upstream
            # actors, then take the checkpoint.
            if len(self.checkpoints_pending) == 0:
                self.logger.debug("Received all checkpoint markers, taking checkpoint for interval %d", self.checkpoint_epoch)
                self._should_checkpoint = True


    def log_push(self, upstream_id, timestamp, records, checkpoint_epoch):
        if len(records) == 1 and records[0] is None:
            self.logger.debug("PUSH: received checkpoint marker %s %d", upstream_id, checkpoint_epoch)
            self.handle_checkpoint_marker(upstream_id, checkpoint_epoch)
            if not self._should_checkpoint:
                # Tell raylet that we haven't finished this task, so that tasks
                # from the same handle are blocked.
                ray.worker.global_worker.notify_task_unfinished()
            return

        if checkpoint_epoch != self.checkpoint_epoch:
            self.logger.info("Received checkpoint %d but we are on checkpoint %d", checkpoint_epoch, self.checkpoint_epoch)
        assert (checkpoint_epoch == self.checkpoint_epoch), "Received checkpoint {} but we are on checkpoint {}".format(checkpoint_epoch, self.checkpoint_epoch)
        if len(records) == 0:
            # This is the last batch that we will receive from this
            # upstream operator.
            for i, flush_buffer in enumerate(self.flush_buffers):
                if len(flush_buffer) > 0:
                    self.flush(i, 0)
            # Send an empty batch. Block on the result to notify the
            # upstream operator when we are finished processing all of its
            # records.
            ray.get([self.flush(i, 0) for i in range(len(self.flush_buffers))])
        else:
            #for i in range(0, len(records), self.batch_size):
            #    batch = records[i * self.batch_size : (i+1) * self.batch_size]
            #    self.process_batch(batch)
            batch_size = self.batch_size
            if self.batch_size is None:
                batch_size = len(records)
            for i in range(0, len(records), batch_size):
                batch = records[i * batch_size : (i+1) * batch_size]
                processed_batch = self.process_batch(timestamp, batch)
                self.num_records_seen += len(batch)

                #for key, flush_buffer in enumerate(processed_batch):
                for i in range(len(processed_batch)):
                    #self.flush_buffers[key] = flush_buffer
                    key = (i + self.operator_index) % self.num_handles
                    flush_buffer = processed_batch[key]
                    self.backpressured_flush(key, timestamp, flush_buffer)


                #for record in records:
                #    records = self.process(record)
                #    # Process the record.
                #    for key, record in records:
                #        self.flush_buffers[key].append(record)
                #    self.num_records_seen += 1

                ## If we are about to take a checkpoint, then force a flush.
                #do_flush = (self.num_records_seen % batch_size == 0) or self._should_checkpoint
                #if do_flush:
                #    for i in range(self.num_handles):
                #        future = self.backpressured_flush(i)
                #        self.logger.debug("Flushing after %d, object %s", self.num_records_seen, future)


    def flush(self, buffer_index, timestamp, event=None):
        if len(self.task_buffer) > 0:
            tasks = [task for _, task, _ in self.task_buffer]
            nondeterministic_events = [event if event is not None else b'' for _, _, event in self.task_buffer]
            ray.worker.global_worker.submit_batch(tasks, nondeterministic_events)
            self.task_buffer.clear()

        flush_buffer = self.flush_buffers[buffer_index]
        future = self.handles[buffer_index].push._remote(
                args=[self.operator_id, timestamp, flush_buffer, self.checkpoint_epoch],
                kwargs={},
                nondeterministic_event=event)
        flush_buffer.clear()
        return future

    def backpressured_flush(self, buffer_index, timestamp, flush_buffer, event=None):
        #flush_buffer = self.flush_buffers[buffer_index]
        args=[self.operator_id, timestamp, flush_buffer, self.checkpoint_epoch]

        batch = self.submit_batch_size > 1
        num_return_vals = 0
        if self.max_queue_length > 0 and self.num_flushes[buffer_index] % self.max_queue_length  == 0:
            num_return_vals = 1

        handle = self.handles[buffer_index]
        task = handle.push._remote(
                args=args,
                kwargs={},
                num_return_vals=num_return_vals,
                nondeterministic_event=event,
                batch=batch)

        self.num_flushes[buffer_index] += 1
        self.num_total_flushes += 1

        if batch:
            self.task_buffer.append((buffer_index, task, event))
            if self.num_total_flushes % self.submit_batch_size == 0:
                tasks = [task for _, task, _ in self.task_buffer]
                nondeterministic_events = [event if event is not None else b'' for _, _, event in self.task_buffer]
                returns = ray.worker.global_worker.submit_batch(tasks, nondeterministic_events)
                self.logger.debug("Submitted task batch %s", len(tasks))
                for i, submit in enumerate(self.task_buffer):
                    buffer_index = submit[0]
                    if returns[i] is not None:
                        self.queues[buffer_index].append(returns[i])
                        wait_queue(self.logger, self.queues[buffer_index], 1)
                self.task_buffer.clear()
        else:
            obj_id = task
            if obj_id is not None:
                self.queues[buffer_index].append(obj_id)
                wait_queue(self.logger, self.queues[buffer_index], 1)

        self.logger.debug("Flushed handle %d after %d", buffer_index, self.num_records_seen)

    def get_pid(self):
        return os.getpid()

    def should_checkpoint(self, checkpoint_context):
        should_checkpoint = self._should_checkpoint
        self._should_checkpoint = False
        return should_checkpoint

    def save_state(self):
        return None

    def load_state(self, state):
        self.state = state

    def save_checkpoint(self, actor_id, checkpoint_id):
        with ray.profiling.profile("save_checkpoint"):
            assert len(self.task_buffer) == 0, (self.operator_id, self.task_buffer)
            start = time.time()
            self.logger.info("Saving checkpoint %d %s", self.checkpoint_epoch, checkpoint_id)
            assert len(self.checkpoints_pending) == 0

            checkpoint = {
                    attr: getattr(self, attr) for attr in self.checkpoint_attrs
                    }
            checkpoint["state"] = self.save_state()
            checkpoint["checkpoint_id"] = checkpoint_id
            checkpoint = pickle.dumps(checkpoint)
            self.logger.debug("Checkpoint size is %d", len(checkpoint))
            # NOTE: The default behavior is to register a random actor handle
            # whenever a handle is pickled, so that the execution dependency is
            # never removed and anytime the handle is unpickled, we will be able to
            # submit tasks.  However, we do not need to do this since we are only
            # going to unpickle the handle once, when the actor recovers from the
            # checkpoint.
            [handle._ray_new_actor_handles.clear() for handle in self.handles]
            checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), self.checkpoint_epoch)
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
            with open(checkpoint_path, 'wb+') as f:
                f.write(checkpoint)

            self.logger.debug("Checkpoint %d took %f", self.checkpoint_epoch, time.time() - start)

            self.checkpoint_epoch += 1
            self.push_checkpoint_marker()

    def push_checkpoint_marker(self):
        checkpoint_marker_args = [self.operator_id, 0, [None], self.checkpoint_epoch]
        self.logger.debug("Pushing checkpoint marker %d", self.checkpoint_epoch)
        for handle in self.handles:
            handle.push._remote(
                    args=checkpoint_marker_args,
                    kwargs={},
                    num_return_vals=0)

    def load_checkpoint(self, actor_id, available_checkpoints):
        self.logger.debug("Available checkpoints %s", available_checkpoints)

        # Get the latest checkpoint that completed.
        checkpoint_tracker = named_actors.get_actor("checkpoint_tracker")
        self.logger.debug("XXX CHECKPOINT counter is %d %s", checkpoint_tracker._ray_actor_counter, checkpoint_tracker._ray_actor_handle_id.hex())
        latest_checkpoint_interval = ray.get(checkpoint_tracker.get_current_epoch.remote())
        self.logger.debug("Latest checkpoint is %d", latest_checkpoint_interval)
        assert latest_checkpoint_interval >= 0, "Actor died before its first checkpoint was taken"
        # Read the latest checkpoint from disk.
        checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), latest_checkpoint_interval)
        checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
        with open(checkpoint_path, 'rb') as f:
            checkpoint = pickle.loads(f.read())
        checkpoint_id = checkpoint.pop('checkpoint_id')
        self.load_state(checkpoint.pop('state'))
        for attr, value in checkpoint.items():
            setattr(self, attr, value)
        self.self_handle.reset_handle_id()
        [handle.reset_handle_id() for handle in self.handles]

        assert self.checkpoint_epoch == latest_checkpoint_interval
        self.checkpoint_epoch += 1
        self.push_checkpoint_marker()
        # Try to process the records that were in the buffer.
        #for upstream_id, record, checkpoint_epoch in checkpoint["buffer"]:
        #    self.replay_push(upstream_id, record, checkpoint_epoch)

        self.logger.info("Reloading checkpoint %d %s", latest_checkpoint_interval, checkpoint_id)
        return checkpoint_id

    def checkpoint_expired(self, actor_id, checkpoint_id):
        return

@ray.remote(max_reconstructions=100)
class Mapper(NondeterministicOperator):
    def __init__(self, words_file, *args):
        import gc
        gc.disable()

        super().__init__(*args)
        self.logger.info("MAPPER: %s", self.operator_id)

        #self.partition = {}
        #with open(words_file, 'r') as f:
        #    for line in f.readlines():
        #        word = line.strip().encode('ascii')
        #        self.partition[word] = hash(word) % self.num_handles

    def key(self, word):
        return hash(word) % self.num_handles

    #def process(self, record):
    #    timestamp, line = record
    #    words = line.split(b' ')
    #    return [(self.key(word), (timestamp, word, 1)) for word in words]

    def process_batch(self, timestamp, batch):
        return cython_process_batch_map(batch, self.num_handles)

    #def process_batch(self, batch):
    #    keyed_counts = {
    #            key: [] for key in range(self.num_handles)
    #            }
    #    with ray.profiling.profile("counts"):
    #        for timestamp, row in batch:
    #            for word in row.split(b' '):
    #                keyed_counts[hash(word) % self.num_handles].append((timestamp, (word, 1)))

    #    return keyed_counts


@ray.remote(max_reconstructions=100)
class Reducer(NondeterministicOperator):
    def __init__(self, *args):
        super().__init__(*args)
        self.state = {}
        self.logger.info("REDUCER: %s", self.operator_id)

    def process_batch(self, timestamp, records):
        assert len(records) == 1
        sink_output = []
        word = cython_process_batch_reducer(self.state, records[0])
        if timestamp > 0:
            sink_output.append([word])
        return sink_output

    #def process_batch(self, timestamps, records):
    #    new_counts = []
    #    for timestamp, record in batch:
    #        word, count = record
    #        if word not in self.state:
    #            self.state[word] = 0
    #        self.state[word] += count
    #        if timestamp > 0:
    #            new_counts.append((timestamp, (word, self.state[word])))
    #    self.logger.debug("REDUCE, batch size: %d, new counts: %d", len(batch), len(new_counts))
    #    return {
    #            0: new_counts,
    #            }

    def process(self, record):
        timestamp, word, count = record
        if word not in self.state:
            self.state[word] = 0
        self.state[word] += count
        return [(0, (timestamp, word, self.state[word]))]

    def get_counts(self):
        return self.state

    def save_state(self):
        return msgpack.dumps(self.state)

    def load_state(self, state):
        self.state = msgpack.loads(state, max_map_len=300000)

@ray.remote(max_reconstructions=100)
class Sink(NondeterministicOperator):
    def __init__(self, checkpoint_tracker, *args):
        super().__init__(*args)
        self.latencies = []

        self.checkpoint_tracker = checkpoint_tracker
        self.logger.info("SINK: %s", self.operator_id)

        self.checkpoint_attrs.append('checkpoint_tracker')

    #def process(self, record):
    #    timestamp, _, _ = record
    #    timestamp = float(timestamp)
    #    if timestamp > 0:
    #        self.output_file.write('{}\n'.format(time.time() - timestamp))
    #    return []

    def process_batch(self, timestamp, batch):
        if timestamp > 0:
            latency = time.time() - timestamp
            self.logger.info("LATENCY sink %s;timestamp:%f,cur_time:%f,latency:%f", self.operator_id, timestamp, time.time(), latency)
            self.latencies.append(latency)
        return {}

    def push_checkpoint_marker(self):
        self.logger.debug("Sink %s notifying checkpoint tracker checkpoint %d, actor task counter: %d",
                self.operator_id,
                self.checkpoint_epoch - 1,
                self.checkpoint_tracker._ray_actor_counter)
        self.checkpoint_tracker.notify_checkpoint_complete.remote(self.operator_id, self.checkpoint_epoch - 1)

    def save_state(self):
        return self.checkpoint_tracker

    def load_state(self, state):
        self.checkpoint_tracker = state
        self.checkpoint_tracker.reset_handle_id()
        self.logger.debug("LOAD CHECKPOINT counter is %d %s", self.checkpoint_tracker._ray_actor_counter, self.checkpoint_tracker._ray_actor_handle_id.hex())

    def flush_latencies(self):
        return self.latencies

def create_local_node(cluster, i, node_kwargs):
    resource = "Node{}".format(i)
    node_kwargs["resources"] = {resource: 100}
    node = cluster.add_node(**node_kwargs)
    return node, resource

# Always on the head node.
@ray.remote(resources={"Node_0": 1})
class CheckpointTracker(object):
    def __init__(self, sink_keys):
        self.sink_keys = sink_keys
        self.sinks_pending = set(self.sink_keys)
        self.checkpoint_epoch = -1

        logging.basicConfig(level=LOG_LEVEL)
        self.logger = logging.getLogger(__name__)
        self.logger.info("CHECKPOINT_TRACKER")

    def notify_checkpoint_complete(self, sink_key, checkpoint_epoch):
        self.logger.info("Received checkpoint %d from %s, on %d", checkpoint_epoch, sink_key, self.checkpoint_epoch)
        assert checkpoint_epoch == self.checkpoint_epoch + 1

        self.sinks_pending.remove(sink_key)
        # If we have received the checkpoint interval from all sinks, then the
        # checkpoint is complete.
        if len(self.sinks_pending) == 0:
            self.checkpoint_epoch += 1
            self.sinks_pending = set(self.sink_keys)
            self.logger.info("Global checkpoint complete %d", self.checkpoint_epoch)

    def get_current_epoch(self):
        self.logger.info("get current epoch returns %d", self.checkpoint_epoch)
        return self.checkpoint_epoch


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
        '--redis-address',
        default=None,
        type=str,
        help='The address of the redis server.')
    parser.add_argument(
        '--dump',
        default=None,
        type=str,
        help='A filename to dump the task timeline')
    parser.add_argument(
        '--num-mappers',
        default=1,
        type=int,
        help='The number of mappers to use.')
    parser.add_argument(
        '--num-reducers',
        default=1,
        type=int,
        help='The number of reducers to use.')
    parser.add_argument(
        '--num-mappers-per-node',
        default=1,
        type=int,
        help='')
    parser.add_argument(
        '--num-reducers-per-node',
        default=1,
        type=int,
        help='')
    parser.add_argument(
        '--num-mapper-failures',
        default=0,
        type=int,
        help='')
    parser.add_argument(
        '--num-reducer-failures',
        default=0,
        type=int,
        help='')
    parser.add_argument(
        '--checkpoint-interval',
        default=0,
        type=int,
        help='The number of records to process per source in one checkpoint epoch.')
    parser.add_argument(
        '--words-file',
        type=str,
        required=True,
        help='Words file')
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size')
    parser.add_argument(
        '--mapper-submit-batch-size',
        type=int,
        default=1)
    parser.add_argument(
        '--reducer-submit-batch-size',
        type=int,
        default=1)
    parser.add_argument(
        '--reducer-num-batches',
        type=int,
        default=1)
    parser.add_argument(
        '--num-records',
        type=int,
        default=200000,
        help='Number of records to generate')
    parser.add_argument(
        '--max-queue-length',
        type=int,
        default=8,
        help='Queue length')
    parser.add_argument(
        '--flush-probability',
        type=float,
        default=1.0,
        help='The probability of flushing a batch on the nondeterministic operator.')
    parser.add_argument(
        '--latency-file',
        type=str,
        default='latency.txt',
        help='')
    parser.add_argument(
        '--timestamp-interval',
        type=int,
        default=1000,
        help='Each source will output a timestamp after this many records')
    parser.add_argument(
        '--target-throughput',
        type=int,
        default=-1,
        help='')
    parser.add_argument(
        '--gcs-delay-ms',
        default=0,
        help='Delay when writing back to GCS.')
    parser.add_argument(
        '--fail-at',
        type=int,
        default=10,
        help='')
    args = parser.parse_args()

    # Create the checkpoint directory.
    checkpoint_dir = os.path.join(
        CHECKPOINT_DIR, ray.worker.global_worker.task_driver_id.hex())
    try:
        os.makedirs(checkpoint_dir)
    except FileExistsError:
        pass


    # Initialize Ray.
    if args.redis_address is None:
        internal_config = json.dumps({
            "initial_reconstruction_timeout_milliseconds": 200,
            "num_heartbeats_timeout": 10,
            "object_manager_repeated_push_delay_ms": 1000,
            "object_manager_pull_timeout_ms": 1000,
            "gcs_delay_ms": 0,
            "lineage_stash_max_failures": -1,
            "node_manager_forward_task_retry_timeout_milliseconds": 100,
        })

        node_kwargs = {
            "num_cpus": 4,
            "object_store_memory": 10**9,
            "_internal_config": internal_config,
            "resources": {
                "Node_0": 100,
                }
        }

        cluster = Cluster(initialize_head=True, head_node_args=node_kwargs)
        # One source and mapper per mapper node. One reducer per reducer node. One
        # sink.
        mapper_nodes, mapper_resources = [], []
        reducer_nodes, reducer_resources = [], []
        sink_node, sink_resource = None, None
        nodes = []
        i = 1
        for _ in range(args.num_mappers):
            node, resource = create_local_node(cluster, i, node_kwargs)
            mapper_nodes.append(node)
            mapper_resources.append(resource)
            i += 1
        for _ in range(args.num_reducers):
            node, resource = create_local_node(cluster, i, node_kwargs)
            reducer_nodes.append(node)
            reducer_resources.append(resource)
            i += 1
        sink_node, sink_resource = create_local_node(cluster, i, node_kwargs)

        redis_address = cluster.redis_address
        ray.init(redis_address=redis_address)
    else:
        redis_address = args.redis_address
        ray.init(redis_address=redis_address)

        node_resources = []
        nodes = ray.global_state.client_table()
        for node in nodes:
            for resource in node['Resources']:
                if 'Node' in resource and resource != 'Node_0':
                    node_resources.append(resource)
        num_mapper_nodes = args.num_mappers // args.num_mappers_per_node
        num_reducer_nodes = args.num_reducers // args.num_reducers_per_node

        mapper_resources = node_resources[:num_mapper_nodes]
        reducer_resources = node_resources[-num_reducer_nodes:]
        print("Starting mappers on", num_mapper_nodes, "nodes")
        print("Starting reducers on", num_reducer_nodes, "nodes")


    operator_ids = [a + b for a in list(string.ascii_uppercase) for b in list(string.ascii_uppercase)]
    # One source per mapper.
    source_keys = [operator_ids.pop(0) for _ in range(args.num_mappers)]
    mapper_keys = [operator_ids.pop(0) for _ in range(args.num_mappers)]
    reducer_keys = [operator_ids.pop(0) for _ in range(args.num_reducers)]
    # One sink.
    sink_keys = [operator_ids.pop(0) for _ in range(args.num_reducers)]

    checkpoint_tracker = CheckpointTracker.remote(sink_keys)
    named_actors.register_actor("checkpoint_tracker", checkpoint_tracker)

    backpressure = True
    checkpoint_interval = args.checkpoint_interval
    if args.target_throughput > 0:
        backpressure = False
        # No checkpoint set. Checkpoint every 30s.
        if checkpoint_interval == 0:
            checkpoint_interval = args.target_throughput // args.num_mappers * 30
    if checkpoint_interval == 0:
        checkpoint_interval = 100000  # Default checkpoint interval is 100k records/source.
    print("Using a checkpoint interval of", checkpoint_interval, "records")

    # Create the sink.
    #sink_args = [args.latency_file, sink_key, [], args.max_queue_length, [], checkpoint_dir, args.batch_size]
    sinks = []
    for i, sink_key in enumerate(sink_keys):
        resource = reducer_resources[(i+1) % len(reducer_resources)]
        upstream_keys = [reducer_keys[(i+1) % len(reducer_keys)]]
        sink_args = [checkpoint_tracker, i, sink_key, [], args.max_queue_length, upstream_keys, checkpoint_dir, None, 1, [1]]
        print("Starting sink", sink_key, "resource:", resource)
        sink = Sink._remote(
                args=sink_args,
                kwargs={},
                resources={resource: 1})
        sinks.append(sink)
    ray.get([sink.register_self_handle.remote(sink) for sink in sinks])
    sink_handle_tasks = []

    # Create the reducers.
    upstream_keys = mapper_keys
    reducers = []
    for i, reducer_key in enumerate(reducer_keys):
        resource = reducer_resources[i % len(reducer_resources)]
        sink = sinks[(i - 1) % len(sinks)]
        sink_handle = ray.put([sink])
        #reducer_args = [reducer_key, sink_handle, args.max_queue_length, upstream_keys, checkpoint_dir, args.batch_size]
        if backpressure:
            max_queue_length = args.max_queue_length
        else:
            max_queue_length = 0
        reducer_args = [i, reducer_key, sink_handle, max_queue_length, upstream_keys, checkpoint_dir, None, args.reducer_submit_batch_size, [3]]
        print("Starting reducer", reducer_key, "upstream:", upstream_keys, "resource:", resource)
        reducer = Reducer._remote(
                args=reducer_args,
                kwargs={},
                resources={resource: 1})
        reducers.append(reducer)

        sink_handle = ray.get(sink_handle)[0]
        sink_handle_tasks.append(sink.register_upstream_actor_handle_ids.remote([sink_handle._ray_actor_handle_id]))

    ray.get(sink_handle_tasks)
    ray.get([reducer.register_self_handle.remote(reducer) for reducer in reducers])
    reducer_handles = [list() for _ in reducers]

    # Create the intermediate operators.
    mappers = []
    for i, mapper_key in enumerate(mapper_keys):
        resource = mapper_resources[i % len(mapper_resources)]
        upstream_keys = [source_keys[i]]
        mapper_key = mapper_keys[i]

        handles = ray.put(reducers)
        if backpressure:
            mapper_max_queue_length = args.max_queue_length
        else:
            mapper_max_queue_length = 0
        mapper_args = [args.words_file, i, mapper_key, handles, mapper_max_queue_length, upstream_keys, checkpoint_dir, args.batch_size, args.mapper_submit_batch_size, [2]]
        print("Starting mapper", mapper_key, "upstream:", upstream_keys, "resource:", resource)
        mapper = Mapper._remote(
                args=mapper_args,
                kwargs={},
                resources={resource: 1})
        mappers.append(mapper)

        for j, reducer_handle in enumerate(ray.get(handles)):
            reducer_handles[j].append(reducer_handle._ray_actor_handle_id)

    ray.get([reducer.register_upstream_actor_handle_ids.remote(reducer_handles[i]) for i, reducer in enumerate(reducers)])
    ray.get([mapper.register_self_handle.remote(mapper) for mapper in mappers])
    mapper_handles = [list() for _ in mappers]

    # Create the sources.
    sources = []
    for i, source_key in enumerate(source_keys):
        resource = mapper_resources[i % len(mapper_resources)]
        handles = ray.put([mappers[i]])
        if backpressure:
            max_queue_length = args.max_queue_length
        else:
            max_queue_length = 0
        source_args = [i, source_key, handles, max_queue_length, checkpoint_dir, checkpoint_interval, args.words_file, args.timestamp_interval, backpressure, [1]]
        print("Starting source", source_key, "resource:", resource)
        sources.append(WordSource._remote(
            args=source_args,
            kwargs={},
            resources={resource: 1}))

        for j, mapper_handle in enumerate(ray.get(handles)):
            mapper_handles[i].append(mapper_handle._ray_actor_handle_id)
    ray.get([mapper.register_upstream_actor_handle_ids.remote(mapper_handles[i]) for i, mapper in enumerate(mappers)])
    ray.get([source.ping.remote() for source in sources])

    start = time.time()
    num_records = args.num_records // len(sources)
    target_throughput = args.target_throughput // len(sources)
    generators = [source.generate.remote(num_records, args.batch_size, target_throughput=target_throughput) for source in sources]

    def kill_node():
        if args.redis_address is None:
            # Kill and restart mappers and reducers.
            nodes_to_kill = mapper_nodes[:args.num_mapper_failures] + reducer_nodes[:args.num_reducer_failures]
            resources_to_restart = mapper_resources[:args.num_mapper_failures] + reducer_resources[:args.num_reducer_failures]
            for node in nodes_to_kill:
                cluster.remove_node(node)
            for resource in resources_to_restart:
                node_kwargs["resources"] = {resource: 100}
                cluster.add_node(**node_kwargs)
        else:
            nodes = ray.global_state.client_table()
            node_resource = mapper_resources[0]
            nodes = [node for node in nodes if node_resource in node['Resources']]
            assert len(nodes) == 1
            node = nodes[0]
            worker_ip = node['NodeManagerAddress']
            head_ip, _ = redis_address.split(':')
            print("Killing node with resource", node_resource, "on ip", worker_ip)
            command = [
                    "/home/ubuntu/ray/benchmarks/cluster-scripts/kill_worker.sh",
                    head_ip,
                    worker_ip,
                    str(args.gcs_delay_ms),
                    node_resource,
                    ]
            subprocess.Popen(command)


    if args.num_mapper_failures > 0 or args.num_reducer_failures > 0:
        print("Sleeping for {}s".format(args.fail_at))
        time.sleep(args.fail_at)
        kill_node()

    throughputs = ray.get(generators)
    end = time.time()
    print("Elapsed time:", end - start)

    if args.dump is not None:
        events = ray.global_state.chrome_tracing_dump()
        with open(args.dump, "w") as outfile:
            json.dump(events, outfile)

    print("Source throughputs:", throughputs)
    print("Total throughput:", sum(throughputs))
    all_latencies = ray.get([sink.flush_latencies.remote() for sink in sinks])
    for i, latencies in enumerate(all_latencies):
        print("Sink", i, "mean latency:", np.mean(latencies), "max latency:", np.max(latencies))
    all_latencies = [latency for latencies in all_latencies for latency in latencies]
    print("FINAL Mean latency:", np.mean(all_latencies), "max latency:", np.max(all_latencies))
    with open(args.latency_file, 'w+') as f:
        for latency in all_latencies:
            f.write('{}\n'.format(latency))


    all_counts = ray.get([reducer.get_counts.remote() for reducer in reducers])
    counts = Counter()
    for i, count in enumerate(all_counts):
        print("Reducer", i, "had", len(count), "words", sum(value for value in count.values()), "records")
        counts.update(count)
    print("Total counts had", len(counts), "words", sum(value for value in counts.values()), "records")
    #print("Final count is", counts)
