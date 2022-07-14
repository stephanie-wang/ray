import ray
import numpy as np
import time

from ray._private.test_utils import SignalActor


@ray.remote
def f(signal):
    print("start")
    ray.get(signal.wait.remote())
    return "done"
 
# TODO:
# - Log the tasks executed on the driver.
# - Driver should recreate task state deterministically but not yet schedule the task.
@ray.remote
class Driver:
    def __init__(self):
        self.fut = None

    def invoke(self, signal):
        self.fut = f.remote(signal)

    def finish(self):
        return ray.get(self.fut)


signal = SignalActor.remote()
driver = Driver.options(max_restarts=-1, max_task_retries=-1, name="Driver", lifetime="detached").remote()
driver.invoke.remote(signal)

time.sleep(1)
ray.kill(driver, no_restart=False)
time.sleep(1)

signal.send.remote()
print(ray.get(driver.finish.remote()))
