import pytest
from ray.workflow.workflow_actor import workflow_actor


@workflow_actor
class A:
    def __init__(self, x):
        self.x = x

    def foo(self):
        return self.x


def test_simple_workflow_actors():
    from ray import workflow
    workflow.init()

    @workflow.step
    def main():
        a = A.create(10)
        # h = a.ray_actor_handle()
        # print(ray.get(h.b.remote()))
        # print(a.b.step(10))
        return a.foo.step()

    print(main.step().run())


def test_workflow_actors_with_wait():
    from ray import workflow
    workflow.init()

    @workflow.step
    def helper(actors, wait_results):
        return actors, wait_results

    @workflow.step
    def main():
        a = A.create(10)
        b = A.create(15)
        return helper.step([a, b], workflow.wait([a.foo.step(), b.foo.step()]))

    print(main.step().run())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
