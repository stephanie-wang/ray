import ray

import os
import signal

# TODOs:
# - try to support callbacks on actor task failure
#   - save the callback
#   - call the callback on actor task failure
# - try to support context
# - writing some demo code
#   - mimic the current xgboost failure handling code
#   - show how it would work with the new API

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
                lambda handle, args: handle.inc.remote(*args))

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
    ray.get(supervisor.kill_child.remote())
