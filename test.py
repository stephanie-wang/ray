import ray
import numpy as np
import time

from ray._private.test_utils import SignalActor


# Works.
@ray.remote
def f_small():
    return np.random.rand()

# Does not work.
@ray.remote
def f_large():
    return np.zeros(int(1e5))

@ray.remote
class CheckpointStorage:
    def __init__(self):
        self.checkpoint = None

    def save(self, checkpoint):
        if self.checkpoint is None:
            self.checkpoint = checkpoint

    def load(self):
        return self.checkpoint

@ray.remote
class Driver:
    def __init__(self, checkpoint_storage):
        self.fut = None
        self.checkpoint_storage = checkpoint_storage

        checkpoint = ray.get(self.checkpoint_storage.load.remote())
        if checkpoint is not None:
            print("Reloading from checkpoint...")
            for attr, val in checkpoint.items():
                print("Set", attr, ":", val)
                setattr(self, attr, val)
            print("Done.")

    def invoke(self):
        if self.fut is not None:
            return self.fut

        self.fut = f_large.remote()
        ray.get(self.checkpoint_storage.save.remote({"fut": self.fut}))
        # TODO: Actually save a checkpoint.
        ray.save_detached_actor_checkpoint(b"", [self.fut])

        return self.fut


checkpoint_storage = CheckpointStorage.remote()
driver = Driver.options(max_restarts=-1, max_task_retries=-1, name="Driver", lifetime="detached").remote(checkpoint_storage)
fut = ray.get(driver.invoke.remote())
print("before")
print(ray.wait([fut], fetch_local=False))

ray.kill(driver, no_restart=False)
time.sleep(3)

print("after")
print(ray.get(fut))
