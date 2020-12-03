import ray

import os
import signal
import sys

# TODOs:
# - try to support callbacks on actor task failure
#   - handle case where object already failed (maybe)
# - try to support context
#   - [Stephanie] try to support passing the original args
# - allow calling another task as part of the callback, and replacing the
#   original task's future with the new one.
#   - [Kai] replace the original future with a raw value returned by the
#   callback.
# - post the callbacks to the main event loop (for async and multithreaded
#   actors)
# - [Kai] writing some demo code
#   - mimic the current xgboost failure handling code
#   - show how it would work with the new API

# Benefits:
# - For actor handling, get immediate feedback when an actor has died.
# Compared to current system, where you need to repeatedly ping or check the
# actor's status.
# - Easier to save the context (actor handle, arguments, etc) as part of the
# failure handling for an object ref.
# - Handle a failure immediately instead of only receiving the exception once
# you call ray.get.

@ray.remote
def f():
    return "xyz"

@ray.remote
def g(s):
    pass


g.remote(f.remote().on_failure(lambda: "abc"))  # g gets "abc"
g.remote(f.remote().on_failure(lambda: print("oh no")))  # g gets normal exception

g.remote("x").on_failure(lambda arg: print(arg))


@ray.remote
def fail_task():
    sys.exit(-1)

@ray.remote
class Child:
    def __init__(self):
        self.x = 0

    def inc(self, num):
        self.x += num
        return self.x

    def pid(self):
        return os.getpid()

@ray.remote
class Supervisor:
    def __init__(self):
        self.child = Child.remote()
        self.child.on_failure(lambda: print("oh no"))

    def send(self, num):
        self.child.inc.remote(num).on_failure(
                lambda: print("task failed"))

        #self.child.inc.remote(num).on_failure(self.child.inc.remote)

    def kill_child(self):
        pid = ray.get(self.child.pid.remote())
        os.kill(pid, signal.SIGKILL)

@ray.remote
class LoadBalancer:
    def __init__(self):
        self.replicas = [Replica.remote() for _ in range(3)]

    def submit(self):
        replica = self.replicas.pop(0)
        replica.submit.remote().on_failure(
                lambda handle, args: self.submit())
        self.replicas.append(replica)


# Demo ideas:
# - print some metrics when children die
# - resubmit a task on child death - could be same as our current max task
#   retries but more flexible.
# - actor replicas: resubmit a task to a replica upon failure


if __name__ == '__main__':
    ray.init()
    supervisor = Supervisor.remote()
    supervisor.kill_child.remote()
    ray.get(supervisor.send.remote(1))

    ray.get(fail_task.remote().on_failure(lambda: print("non-actor task died")))
