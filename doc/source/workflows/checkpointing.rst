Checkpointing
=============

Introduction
------------

Ray Workflows provides strong fault tolerance and exactly-once execution semantics by checkpointing the workflow DAG before execution and the outputs after execution. However, checkpointing is time consuming, especially when you have large inputs and outputs for workflow steps. When exactly-once execution semantics is not required, you can skip some checkpoints to speed up your workflow.

Besides skipping checkpoints, an alternative is to replace workflow steps with Ray remote functions. But there are 3 reasons against it:

1. You cannot compose a workflow DAG by mixing workflow steps and Ray remote functions.
2. Unlike workflow steps, Ray workflow would not display progress and status of Ray remote functions.
3. The remote functions won’t be considered as part of the workflow, so they won’t be recovered automatically when they fail. For example, instead of returning a workflow (step) B in workflow step A, you return a ObjectRef of a remote function. With workflow step B, workflow enables recovery from step B; but with the remote function, the workflow can only recover from step A.


Semantics of Checkpointing
--------------------------

Understand Checkpoints
~~~~~~~~~~~~~~~~~~~~~~

By default, a workflow step checkpoints its output when finished execution. The typical output is some values returned by a workflow step, for example:


.. code-block:: python

    @workflow.step
    def read_data(num: int):
        return [i for i in range(num)]
        assert handler.double.run(True) == 4


But the output could be more general than a value, this is because a parent workflow may return a child workflow. In the example below, ``book_trip`` returns ``finalize_or_cancel``, which implicitly contains a workflow DAG including all steps inside, their inputs and DAG structure etc. So the output of ``book_trip`` is the workflow DAG inside it, and the output is checkpointed when ``book_trip`` is returned.

.. code-block:: python

    @workflow.step
    def book_trip(origin: str, dest: str, dates) -> "Workflow[Receipt]":
        # Note that the workflow engine will not begin executing
        # child workflows until the parent step returns.
        # This avoids step overlap and ensures recoverability.
        f1: Workflow = book_flight.step(origin, dest, dates[0])
        f2: Workflow = book_flight.step(dest, origin, dates[1])
        hotel: Workflow = book_hotel.step(dest, dates)
        return finalize_or_cancel.step([f1, f2], [hotel])

Skipping checkpointing the DAG inside ``book_trip`` does not mean we cannot recover from a checkpoint produced “inside” ``book_trip``. In this case, ``book_trip`` returns ``finalize_or_cancel``, so if ``finalize_or_cancel`` returns and checkpointed its output (a python value of another DAG), when resuming ``book_trip``, we can resume from this output checkpoint.

For a top-level workflow (e.g., a workflow in a driver script), it's DAG will be ``checkpointed`` before execution. We can imagine there is an invisible “root” workflow step, where the top-level workflow is its output. This also implies we cannot skip checkpointing the DAG of the top-level workflow, which makes sense (otherwise we cannot recover from the failure of the workflow).

Checkpoints Options
~~~~~~~~~~~~~~~~~~~

We control the checkpoints by specify the checkpoint options like this:

.. code-block:: python

    data = read_data.options(checkpoint=False).step(10)


The checkpoint option for a workflow step would be “None”, “True” and “False”. If not specified, it is “None”. The checkpoint option follows the inheriting rule:

1. If the checkpoint option is “None”, the checkpoint option inherits the option of its parent step. If it does not have a parent step (i.e. a step of a top-level workflow), it remains “None”.
2. If the checkpoint option is True/False, it remains unchanged regardless of the options of its parent step.
3. After inheritance, if the checkpoint option is True/None, the step checkpoints its output; if the checkpoint option is False, the step skips checkpointing its output.


Examples
--------

Here we show some examples about checkpointing.

Example 1: Simple Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~

This example shows how to use checkpoint options for a simple workflow, and what are the benefits and side effects of skipping checkpoints.

.. image:: basic.png
   :width: 500px
   :align: center


.. code-block:: python

    @workflow.step
    def read_data(num: int):  return [i for i in range(num)]

    @workflow.step
    def preprocessing(data: List[float]) -> List[float]:
       return [d**2 for d in data]

    @workflow.step
    def aggregate(data: List[float]) -> float: return sum(data)

    data = read_data.step(10)
    preprocessed_data = preprocessing.step(data)
    output = aggregate.step(preprocessed_data)

    # Execute the workflow and print the result.
    print(output.run())


In this example, we checkpoint all inputs and outputs of all workflow steps. This is the default behavior when not specifying checkpointing options.

To skip the output of ``read_data``, we simply need to specify the checkpointing option:

.. code-block:: python

    # ...
    data = read_data.options(checkpoint=False).step(10)
    preprocessed_data = preprocessing.step(data)
    # ...


This avoids saving the output of ``read_data``.

In general, you may want to skip checkpoints because:
1. It speeds up the workflow execution.
2. It saves storage space.
3. Recovery by re-execution would be faster than loading the checkpoints for certain steps.

However, you should also be careful about the side effects. In this example, if ``read_data`` checkpoints, when the workflow fails during executing ``preprocessed_data``, it recovers from the checkpoint, so we always get the exact input for ``preprocessed_data``. Without the checkpoint, we need to re-execute ``read_data``. This may cause undesired behavior especially when ``read_data`` is non-deterministic or has some side-effects.


Example 2: Nested Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~

This example shows how to use checkpoint options for a nested workflow. We explain what is the semantic of skipping the checkpoint of a nested workflow step.

In Ray Workflows, we can define workflow steps dynamically by returning a workflow step inside another step. Here is one example:

.. code-block:: python

    @workflow.step
    def book_flight(...) -> Flight: ...

    @workflow.step
    def book_hotel(...) -> Hotel: ...

    @workflow.step
    def finalize_or_cancel(
       flights: List[Flight],
       hotels: List[Hotel]) -> Receipt: ...

    @workflow.step
    def book_trip(origin: str, dest: str, dates) -> "Workflow[Receipt]":
       # Note that the workflow engine will not begin executing
       # child workflows until the parent step returns.
       # This avoids step overlap and ensures recoverability.
       f1: Workflow = book_flight.step(origin, dest, dates[0])
       f2: Workflow = book_flight.step(dest, origin, dates[1])
       hotel: Workflow = book_hotel.step(dest, dates)
       return finalize_or_cancel.step([f1, f2], [hotel])

    fut = book_trip.step("OAK", "SAN", ["6/12", "7/5"])
    fut.run()  # returns Receipt(...)


You can skip the checkpoint of steps inside a workflow step. The behavior is exactly the same as example 1. For example, in this case, all output checkpoints produced by steps inside ``book_trip`` are skipped:


.. code-block:: python

    @workflow.step
    def book_trip(origin: str, dest: str, dates) -> "Workflow[Receipt]":
       f1: Workflow = book_flight.options(checkpoint=False).step(origin, dest, dates[0])
       f2: Workflow = book_flight.options(checkpoint=False).step(dest, origin, dates[1])
       hotel: Workflow = book_hotel.options(checkpoint=False).step(dest, dates)
       return finalize_or_cancel.options(checkpoint=False).step([f1, f2], [hotel])



However, there is a difference when skipping checkpoints of a step that contains other steps. In this example, the step is ``book_trip``:


.. code-block:: python

    @workflow.step
    def book_trip(origin: str, dest: str, dates) -> "Workflow[Receipt]":
       f1: Workflow = book_flight.step(origin, dest, dates[0])
       f2: Workflow = book_flight.step(dest, origin, dates[1])
       hotel: Workflow = book_hotel.step(dest, dates)
       return finalize_or_cancel.step([f1, f2], [hotel])

    fut = book_trip.options(checkpoint=False).step("OAK", "SAN", ["6/12", "7/5"])
    fut.run()  # returns Receipt(...)

When skipping the checkpoint of ``book_trip``, it skips its output - the workflow DAG defined inside it. By default, ``book_trip`` checkpoints the DAG inside it (including all steps, all inputs to the steps, how these steps are organized together, etc) for recovery. So skipping the output of ``book_trip`` means you must rerun ``book_trip`` from the beginning when you recover from the failure, unless all steps inside ``book_trip`` have finished execution and checkpointed.

The steps inside a workflow step inherits the checkpoint option, if not checkpoint option is specified. This propagates recursively, if steps inside also have steps inside. So the example above is equivalent to:


.. code-block:: python

    @workflow.step
    def book_trip(origin: str, dest: str, dates) -> "Workflow[Receipt]":
       f1: Workflow = book_flight.options(checkpoint=False).step(origin, dest, dates[0])
       f2: Workflow = book_flight.options(checkpoint=False).step(dest, origin, dates[1])
       hotel: Workflow = book_hotel.options(checkpoint=False).step(dest, dates)
       return finalize_or_cancel.options(checkpoint=False).step([f1, f2], [hotel])

    fut = book_trip.options(checkpoint=False).step("OAK", "SAN", ["6/12", "7/5"])
    fut.run()  # returns Receipt(...)


Although the DAG is skipped, we can save the outputs of steps inside a step whose checkpoint is skipped, by specifying `checkpoint=True`. In this example, there are 4 steps inside ``book_trip``. Saving the output of all steps except ``finalize_or_cancel`` is meaningless, because their checkpoints cannot be reused during recovery, since we do not save the DAG. In this case, workflow would raise an exception, saying such checkpoints do not make sense. If we save the output checkpoint of ``finalize_or_cancel``, then after ``finalize_or_cancel`` finishes, we can recover from the checkpointed output of ``finalize_or_cancel`` instead of rerunning ``book_trip``.


Example 3: Recursive Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Recursive workflows are special examples of nested workflows. Here we show how we can implement periodic checkpointing by making use of checkpoint options.

This example includes a step that calculates the exponential of a big array using recursion. Because the array serves as the input for step ``exp``, it is checkpointed every time with the DAG inside ``exp``. For performance reasons, we may want to just checkpoint a fraction of them.

.. code-block:: python

    @workflow.step
    def exp(array, n, _):
        if n == 0:
           return array
        # simulate a dependence
        dep = foo.step(n)
        return exp.step(array * 2, n - 1, foo)

Here is how we can modify it for periodic checkpointing:

.. code-block:: python

    @workflow.step
    def exp(array, n, _):
        if n == 0:
           return array
        if n % 10 == 0:
          should_checkpoint=True
        else:
          should_checkpoint=False

        dep = foo.step()
        return exp.options(checkpoint=should_checkpoint).step(array * 2, n - 1, foo)


As discussed in example 2, users are not allowed to checkpoint the output of intermediate steps like ``foo``:

.. code-block:: python

    @workflow.step
    def exp(array, n, _):
        if n == 0:
           return array
        if n % 10 == 0:
          should_checkpoint=True
        else:
          should_checkpoint=False

        dep = foo.options(checkpoint=True).step()  # Error
        return exp.options(checkpoint=should_checkpoint).step(array * 2, n - 1, foo)
