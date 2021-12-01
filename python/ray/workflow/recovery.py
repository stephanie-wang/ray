from typing import List, Any, Union, Dict, Callable, Tuple, Optional

import ray
from ray.workflow import workflow_context
from ray.workflow import serialization
from ray.workflow.common import (Workflow, StepID, WorkflowRef,
                                 WorkflowStaticRef, WorkflowExecutionResult,
                                 StepType, WorkflowActorBase)
from ray.workflow import storage
from ray.workflow import workflow_storage
from ray.workflow.step_function import WorkflowStepFunction


class WorkflowStepNotRecoverableError(Exception):
    """Raise the exception when we find a workflow step cannot be recovered
    using the checkpointed inputs."""

    def __init__(self, step_id: StepID):
        self.message = f"Workflow step[id={step_id}] is not recoverable"
        super().__init__(self.message)


class WorkflowNotResumableError(Exception):
    """Raise the exception when we cannot resume from a workflow."""

    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] is not resumable."
        super().__init__(self.message)


@WorkflowStepFunction
def _recover_workflow_step(args: List[Any], kwargs: Dict[str, Any],
                           input_workflows: List[Any],
                           input_workflow_refs: List[WorkflowRef]):
    """A workflow step that recovers the output of an unfinished step.

    Args:
        args: The positional arguments for the step function.
        kwargs: The keyword args for the step function.
        input_workflows: The workflows in the argument of the (original) step.
            They are resolved into physical objects (i.e. the output of the
            workflows) here. They come from other recover workflows we
            construct recursively.

    Returns:
        The output of the recovered step.
    """
    reader = workflow_storage.get_workflow_storage()
    step_id = workflow_context.get_current_step_id()
    func: Callable = reader.load_step_func_body(step_id)
    return func(*args, **kwargs)


def _reconstruct_wait_step(
        reader: workflow_storage.WorkflowStorage, step_id: StepID,
        result: workflow_storage.StepInspectResult,
        input_map: Dict[StepID, Any], actor_map: Dict[str, WorkflowActorBase]):
    input_workflows = []
    step_options = result.step_options
    wait_options = step_options.ray_options.get("wait_options", {})
    for i, _step_id in enumerate(result.workflows):
        # Check whether the step has been loaded or not to avoid
        # duplication
        if _step_id in input_map:
            r = input_map[_step_id]
        else:
            r = _construct_resume_workflow_from_step(reader, _step_id,
                                                     input_map, actor_map)
            input_map[_step_id] = r
        if isinstance(r, Workflow):
            input_workflows.append(r)
        else:
            assert isinstance(r, StepID)
            # TODO (Alex): We should consider caching these outputs too.
            output = reader.load_step_output(r)
            # Simulate a workflow with a workflow reference so it could be
            # used directly by 'workflow.wait'.
            static_ref = WorkflowStaticRef(step_id=r, ref=ray.put(output))
            wf = Workflow.from_ref(static_ref)
            input_workflows.append(wf)

    from ray import workflow
    wait_step = workflow.wait(input_workflows, **wait_options)
    # override step id
    wait_step._step_id = step_id
    return wait_step


def _reconstruct_workflow_actor(reader: workflow_storage.WorkflowStorage,
                                actor_id: str, state_index: int):
    from ray.workflow.workflow_actor import WorkflowActorClass
    base_class = reader.load_physical_actor_class_body(actor_id)
    actor_cls = WorkflowActorClass._from_class(base_class)
    args, kwargs = reader.load_physical_actor_state(actor_id, 0)
    actor = actor_cls._create(actor_id, None, *args, **kwargs)
    # FIXME(suquark): This is a temporary patch for a race condition issue.
    if state_index > 0:
        while True:
            try:
                state = reader.load_physical_actor_state(actor_id, state_index)
                break
            except Exception:
                state_index -= 1
                assert state_index >= 0
        actor.ray_actor_handle().__setstate__.remote(state)
    return actor


def _reconstruct_workflow_actor_step(
        reader: workflow_storage.WorkflowStorage, step_id: StepID,
        result: workflow_storage.StepInspectResult,
        input_map: Dict[StepID, Any], actor_map: Dict[str, WorkflowActorBase]):
    workflow_actor_options = result.step_options.ray_options[
        "workflow_actor_options"]
    actor_id = workflow_actor_options["actor_id"]
    state_index = workflow_actor_options["state_index"]
    method_name = workflow_actor_options["method_name"]
    if actor_id in actor_map:
        assert actor_map[actor_id].state_index() == state_index
    else:
        print(f"[recovery] Reconstruct actor {actor_id} with "
              f"state_index={state_index} for actor step {step_id}.")
        actor_map[actor_id] = _reconstruct_workflow_actor(
            reader, actor_id, state_index)
    actor = actor_map[actor_id]
    actor_step_method = getattr(actor, method_name)
    # TODO(suquark): load dependence
    args, kwargs = reader.load_step_args(
        step_id, workflows=[], workflow_refs=[], workflow_actors=[])
    actor_step = actor_step_method.step(*args, **kwargs)
    # override step id
    actor_step._step_id = step_id
    return actor_step


def _construct_resume_workflow_from_step(
        reader: workflow_storage.WorkflowStorage, step_id: StepID,
        input_map: Dict[StepID, Any],
        actor_map: Dict[str, WorkflowActorBase]) -> Union[Workflow, StepID]:
    """Try to construct a workflow (step) that recovers the workflow step.
    If the workflow step already has an output checkpointing file, we return
    the workflow step id instead.

    Args:
        reader: The storage reader for inspecting the step.
        step_id: The ID of the step we want to recover.
        input_map: This is a context storing the input which has been loaded.
            This context is important for dedupe

    Returns:
        A workflow that recovers the step, or a ID of a step
        that contains the output checkpoint file.
    """
    result: workflow_storage.StepInspectResult = reader.inspect_step(step_id)
    if result.output_object_valid:
        # we already have the output
        return step_id
    if isinstance(result.output_step_id, str):
        return _construct_resume_workflow_from_step(
            reader, result.output_step_id, input_map, actor_map)
    # output does not exists or not valid. try to reconstruct it.
    if not result.is_recoverable():
        raise WorkflowStepNotRecoverableError(step_id)

    step_options = result.step_options
    # Process the wait step as a special case.
    if step_options.step_type == StepType.WAIT:
        return _reconstruct_wait_step(reader, step_id, result, input_map,
                                      actor_map)
    elif step_options.step_type == StepType.PHYSICAL_ACTOR_METHOD:
        return _reconstruct_workflow_actor_step(reader, step_id, result,
                                                input_map, actor_map)

    with serialization.objectref_cache():
        input_workflows = []
        for i, _step_id in enumerate(result.workflows):
            # Check whether the step has been loaded or not to avoid
            # duplication
            if _step_id in input_map:
                r = input_map[_step_id]
            else:
                r = _construct_resume_workflow_from_step(
                    reader, _step_id, input_map, actor_map)
                input_map[_step_id] = r
            if isinstance(r, Workflow):
                input_workflows.append(r)
            else:
                assert isinstance(r, StepID)
                # TODO (Alex): We should consider caching these outputs too.
                input_workflows.append(reader.load_step_output(r))
        workflow_refs = list(map(WorkflowRef, result.workflow_refs))

        # reconstruct workflow actor inputs
        input_workflow_actors = []
        for i, (actor_id, state_index) in enumerate(result.workflow_actors):
            if actor_id in actor_map:
                # TODO(suquark): In theory we should check if the actor
                # state is consistent with the state recorded in the step.
                # But it could be possible that some workflow actor step
                # is scheduled before this step, so the actor state is
                # already updated. So we just use the actor without
                # checking its state.
                #
                # assert actor_map[actor_id].state_index() == state_index, (
                #     f"Assertion failed when recovering step {step_id}. "
                #     f"Expected state index = {state_index} for "
                #     f"actor {actor_id}, but get "
                #     f"{actor_map[actor_id].state_index()}")
                print(f"[recovery] Use reconstructed actor {actor_id} "
                      f"with state_index={actor_map[actor_id].state_index()} "
                      f"for workflow step {step_id}.")
            else:
                print(f"[recovery] Reconstruct actor {actor_id} with "
                      f"state_index={state_index} for workflow step "
                      f"{step_id}.")
                actor_map[actor_id] = _reconstruct_workflow_actor(
                    reader, actor_id, state_index)
            input_workflow_actors.append(actor_map[actor_id])

        args, kwargs = reader.load_step_args(
            step_id,
            input_workflows,
            workflow_refs,
            workflow_actors=input_workflow_actors)
        recovery_workflow: Workflow = _recover_workflow_step.step(
            args, kwargs, input_workflows, workflow_refs)
        recovery_workflow._step_id = step_id
        # override step_options
        recovery_workflow.data.step_options = step_options
        return recovery_workflow


@ray.remote
class ResumeWorkflowStepExecutor:
    @ray.method(num_returns=2)
    def _resume_workflow_step_executor(
            self, workflow_id: str, step_id: "StepID", store_url: str,
            current_output: [ray.ObjectRef
                             ]) -> Tuple[ray.ObjectRef, ray.ObjectRef]:
        # TODO (yic): We need better dependency management for virtual actor
        # The current output will always be empty for normal workflow
        # For virtual actor, if it's not empty, it means the previous job is
        # running. This is a really bad one.
        for ref in current_output:
            try:
                while isinstance(ref, ray.ObjectRef):
                    ref = ray.get(ref)
            except Exception:
                pass
        try:
            store = storage.create_storage(store_url)
            wf_store = workflow_storage.WorkflowStorage(workflow_id, store)
            r = _construct_resume_workflow_from_step(wf_store, step_id, {}, {})
        except Exception as e:
            raise WorkflowNotResumableError(workflow_id) from e

        if isinstance(r, Workflow):
            with workflow_context.workflow_step_context(
                    workflow_id, store.storage_url,
                    last_step_of_workflow=True):
                from ray.workflow.step_executor import execute_workflow
                result = execute_workflow(r)
                del r
                return result.persisted_output, result.volatile_output
        assert isinstance(r, StepID)
        return wf_store.load_step_output(r), None


_actors = []


def resume_workflow_step(
        workflow_id: str, step_id: "StepID", store_url: str,
        current_output: Optional[ray.ObjectRef]) -> WorkflowExecutionResult:
    """Resume a step of a workflow.

    Args:
        workflow_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        step_id: The step to resume in the workflow.
        store_url: The url of the storage to access the workflow.

    Raises:
        WorkflowNotResumableException: fail to resume the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    if current_output is None:
        current_output = []
    else:
        current_output = [current_output]

    a = ResumeWorkflowStepExecutor.remote()
    _actors.append(a)
    persisted_output, volatile_output = (
        a._resume_workflow_step_executor.remote(workflow_id, step_id,
                                                store_url, current_output))
    return WorkflowExecutionResult(persisted_output, volatile_output)


def get_latest_output(workflow_id: str, store: storage.Storage) -> Any:
    """Get the latest output of a workflow. This function is intended to be
    used by readonly virtual actors. To resume a workflow,
    `resume_workflow_job` should be used instead.

    Args:
        workflow_id: The ID of the workflow.
        store: The storage of the workflow.

    Returns:
        The output of the workflow.
    """
    reader = workflow_storage.WorkflowStorage(workflow_id, store)
    try:
        step_id: StepID = reader.get_latest_progress()
        while True:
            result: workflow_storage.StepInspectResult = reader.inspect_step(
                step_id)
            if result.output_object_valid:
                # we already have the output
                return reader.load_step_output(step_id)
            if isinstance(result.output_step_id, str):
                step_id = result.output_step_id
            else:
                raise ValueError(
                    "Workflow output does not exists or not valid.")
    except Exception as e:
        raise WorkflowNotResumableError(workflow_id) from e
