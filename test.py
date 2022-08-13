import ray
import numpy as np
import time

from ray._private.test_utils import SignalActor


@ray.remote
def f(signal):
    val = np.random.rand()
    print("start", val)
    ray.get(signal.wait.remote())
    return ("done", val)


@ray.remote
class Log:
    def __init__(self):
        self.log = None

    def log(self, method, args):
        if self.log is None:
            self.log = []
        self.log.append((method, args))

    def read(self):
        return self.log
 
# TODO:
# - Log the tasks executed on the driver.
# - Driver should recreate task state deterministically but not yet schedule the task.
@ray.remote
class Driver:
    def __init__(self, log):
        self.fut = None
        self.log = log

        entries = ray.get(self.log.read.remote())
        if entries is not None:
            for method, args in entries:
                getattr(self, method)(*args)

    def invoke(self, signal):
        ray.get(self.log.log.remote("invoke", [signal]))
        self.fut = f.remote(signal)
        print(self.fut)

    def finish(self):
        ray.get(self.log.log.remote("finish", []))
        return ray.get(self.fut)


signal = SignalActor.remote()
log = Log.remote()
driver = Driver.options(max_restarts=-1, max_task_retries=-1, name="Driver", lifetime="detached").remote(log)
driver.invoke.remote(signal)

time.sleep(1)
ray.kill(driver, no_restart=False)
time.sleep(1)

signal.send.remote()
print(ray.get(driver.finish.remote()))
