import abc
import copy
import functools
import inspect
import logging
import uuid
import weakref
from typing import TYPE_CHECKING, Any, Dict
import ray
from ray.util.inspect import (is_function_or_method, is_class_method,
                              is_static_method)
from ray._private import signature

from ray.workflow.common import (WorkflowData, Workflow, StepType,
                                 WorkflowStepRuntimeOptions, WorkflowActorBase)
from ray.workflow import serialization_context
from ray.workflow import workflow_storage

if TYPE_CHECKING:
    from ray.workflow.common import StepID
    from ray.workflow.workflow_context import WorkflowStepContext
    from ray.workflow.step_executor import _BakedWorkflowInputs

logger = logging.getLogger(__name__)

PATCHED_PREFIX = "__patched__"


def _patch_class(base_class: type, ignore_init: bool):
    """Patch the class so it would be compatible with workflows."""
    actor_methods = inspect.getmembers(base_class, is_function_or_method)

    # make a copy, so we won't modify the original method
    cls = copy.copy(base_class)
    if ignore_init:

        def __init__(self):
            pass

        cls.__init__ = __init__

    from ray.workflow.step_executor import _workflow_step_executor
    for method_name, _method in actor_methods:
        if method_name not in ("__init__", "__getstate__", "__setstate__"):
            # readonly = getattr(method, "__actor_readonly__", False)
            def _func(self, actor_id, state_index, method,
                      context: "WorkflowStepContext", step_id: "StepID",
                      baked_inputs: "_BakedWorkflowInputs",
                      runtime_options: "WorkflowStepRuntimeOptions"):
                f = functools.partial(method, self)
                result = _workflow_step_executor(f, context, step_id,
                                                 baked_inputs, runtime_options)

                from ray.workflow import workflow_context
                if runtime_options.checkpoint:
                    workflow_context.update_workflow_step_context(
                        context, step_id)
                    store = workflow_storage.get_workflow_storage(
                        context.workflow_id)
                    store.save_physical_actor_state(actor_id, state_index,
                                                    self.__getstate__())

                return result

            setattr(cls, PATCHED_PREFIX + method_name, _func)

    return cls


class ActorMethodBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def step(self, *args, **kwargs) -> Workflow:
        """Create a workflow step from the actor.

        Returns:
            A workflow step object.
        """


# Create objects to wrap method invocations. This is done so that we can
# invoke methods with actor.method.run() instead of actor.method().
class ActorMethod(ActorMethodBase):
    """A class used to invoke an actor method.

    Note: This class only keeps a weak ref to the actor, unless it has been
    passed to a remote function. This avoids delays in GC of the actor.

    Attributes:
        _workflow_actor: A waekref to the actor.
        _method_name: The name of the actor method.
    """

    def __init__(self, workflow_actor, original_class, method_name: str,
                 runtime_options: WorkflowStepRuntimeOptions):
        self._workflow_actor = weakref.ref(workflow_actor)
        self._method_name = method_name
        self._original_class = original_class
        self._original_method = getattr(original_class, method_name)
        # Extract the signature of the method. This will be used
        # to catch some errors if the methods are called with inappropriate
        # arguments.

        # Whether or not this method requires binding of its first
        # argument. For class and static methods, we do not want to bind
        # the first argument, but we do for instance methods
        method = inspect.unwrap(self._original_method)
        is_bound = (is_class_method(method)
                    or is_static_method(original_class, method_name))

        # Print a warning message if the method signature is not
        # supported. We don't raise an exception because if the actor
        # inherits from a class that has a method whose signature we
        # don't support, there may not be much the user can do about it.
        self._signature = signature.extract_signature(
            method, ignore_first=not is_bound)

        self._options = runtime_options

    def __call__(self, *args, **kwargs):
        raise TypeError("Actor methods cannot be called directly. Instead "
                        f"of running 'object.{self._method_name}()', try "
                        f"'object.{self._method_name}.remote()'.")

    def step(self, *args, **kwargs) -> Workflow:
        """Create a workflow step from the actor.

        Returns:
            A workflow step object.
        """

        flattened_args = signature.flatten_args(self._signature, args, kwargs)
        workflow_inputs = serialization_context.make_workflow_inputs(
            flattened_args)

        self._options = WorkflowStepRuntimeOptions.make(
            step_type=StepType.PHYSICAL_ACTOR_METHOD,
            checkpoint=None,
            ray_options={
                "workflow_actor_options": {
                    "actor_id": self._workflow_actor().actor_id(),
                    # TODO(suquark): The state index may not be the actual
                    # index the step is using.
                    # Currently I think it is consistent.
                    "state_index": self._workflow_actor().state_index(),
                    "method_name": self._method_name,
                    "actor_method_name": PATCHED_PREFIX + self._method_name,
                }
            })

        # TODO(suquark): handle the case where options are directly passed to
        # the class method decorator. We havn't support class method decorator
        # yet.
        from ray.workflow.workflow_context import inherit_checkpoint_context
        self._options.checkpoint = inherit_checkpoint_context(
            self._options.checkpoint)

        workflow_data = WorkflowData(
            func_body=self._original_method,
            inputs=workflow_inputs,
            name=None,
            step_options=self._options,
            user_metadata={},
        )

        wf = Workflow(workflow_data)
        # Shipping the actor with the workflow. This is a temporary solution.
        wf._workflow_actor = self._workflow_actor()
        return wf

    def options(self,
                *,
                max_retries: int = 1,
                catch_exceptions: bool = False,
                name: str = None,
                metadata: Dict[str, Any] = None,
                **ray_options) -> ActorMethodBase:
        """This function set how the actor method is going to be executed.

        Args:
            max_retries: num of retries the step for an application
                level error.
            catch_exceptions: Whether the user want to take care of the
                failure manually.
                If it's set to be true, (Optional[R], Optional[E]) will be
                returned.
                If it's false, the normal result will be returned.
            name: The name of this step, which will be used to
                generate the step_id of the step. The name will be used
                directly as the step id if possible, otherwise deduplicated by
                appending .N suffixes.
            metadata: metadata to add to the step.
            **ray_options: All parameters in this fields will be passed
                to ray remote function options.

        Returns:
            The actor method itself.
        """
        raise NotImplementedError

    def __getstate__(self):
        return {
            "actor_handle": self._actor_handle,
            "original_class": self._original_class,
            "method_name": self._method_name,
            "runtime_options": self._options,
        }

    def __setstate__(self, state):
        self.__init__(state["actor_handle"], state["original_class"],
                      state["method_name"], state["runtime_options"])


class ActorMetadata:
    """Recording the metadata of a virtual actor class, including
    the signatures of its methods etc."""

    def __init__(self, original_class: type):
        actor_methods = inspect.getmembers(original_class,
                                           is_function_or_method)

        self.cls = original_class
        self.module = original_class.__module__
        self.name = original_class.__name__
        self.qualname = original_class.__qualname__
        self.methods = {}
        for method_name, method in actor_methods:
            self.methods[method_name] = method


class WorkflowActorClassBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create(self, actor_id: str, *args, **kwargs) -> "WorkflowActor":
        """Create a workflow actor.

        Args:
            actor_id: The ID of the actor.
            args: These arguments are forwarded directly to the actor
                constructor.
            kwargs: These arguments are forwarded directly to the actor
                constructor.

        Returns:
            A handle to the newly created actor.
        """


class WorkflowActorClass(WorkflowActorClassBase):
    """The workflow actor class used to create a workflow actor."""
    _metadata: ActorMetadata
    _ray_actor: ray.actor.ActorClass

    def __init__(self):
        # This shouldn't be reached because one of the base classes must be
        # an actor class if this was meant to be subclassed.
        assert False, (
            "WorkflowActorClass.__init__ should not be called. Please use "
            "the @workflow.actor decorator instead.")

    def __call__(self, *args, **kwargs):
        """Prevents users from directly instantiating an ActorClass.

        This will be called instead of __init__ when 'VirtualActorClass()' is
        executed because an is an object rather than a metaobject. To properly
        instantiated a virtual actor, use 'VirtualActorClass.create()' or
        'VirtualActorClass.get()'.

        Raises:
            Exception: Always.
        """
        raise TypeError("Actors cannot be instantiated directly. "
                        f"Instead of '{self._metadata.name}()', "
                        f"use '{self._metadata.name}.create()'.")

    @classmethod
    def _from_class(cls, base_class: type,
                    ignore_init: bool = False) -> "WorkflowActorClass":
        """Construct the virtual actor class from a base class.

        ignore_init: If True, replace the original "__init__"
        with an no-op function. This is mainly used for reconstructing the
        actor from state.
        """
        # TODO(suquark): we may use more complex name for private functions
        # to avoid collision with user-defined functions.
        for attribute in [
                "create",
                "_create",
                "option",
                "_construct",
                "_from_class",
        ]:
            if hasattr(base_class, attribute):
                logger.warning("Creating an actor from class "
                               f"{base_class.__name__} overwrites "
                               f"attribute {attribute} of that class")

        if not is_function_or_method(getattr(base_class, "__init__", None)):
            # Add __init__ if it does not exist.
            # Actor creation will be executed with __init__ together.

            # Assign an __init__ function will avoid many checks later on.
            def __init__(self):
                pass

            base_class.__init__ = __init__

        # Make sure the actor class we are constructing inherits from the
        # original class so it retains all class properties.
        class DerivedActorClass(cls, base_class):
            pass

        metadata = ActorMetadata(base_class)
        has_getstate = "__getstate__" in metadata.methods
        has_setstate = "__setstate__" in metadata.methods

        if not has_getstate and not has_setstate:
            # This is OK since we'll use default one defined
            pass
        elif not has_getstate:
            raise ValueError("The class does not have '__getstate__' method")
        elif not has_setstate:
            raise ValueError("The class does not have '__setstate__' method")

        DerivedActorClass.__module__ = metadata.module
        name = f"WorkflowActorClass({metadata.name})"
        DerivedActorClass.__name__ = name
        DerivedActorClass.__qualname__ = name
        # Construct the base object.

        self = DerivedActorClass.__new__(DerivedActorClass)

        self._metadata = metadata
        self._wrapped_cls = _patch_class(base_class, ignore_init)
        # TODO(suquark): Add Ray options for the actor.
        self._ray_actor = ray.remote(self._wrapped_cls)

        return self

    def create(self, *args, _ray_options=None, _actor_id=None,
               **kwargs) -> "WorkflowActor":
        """Create a workflow actor."""
        _actor_id = _actor_id or uuid.uuid4().hex
        # TODO(suquark): Save "ray_options".
        from ray.workflow.workflow_context import inherit_checkpoint_context
        # TODO(suquark): This is a temporary way of getting checkpointing
        # options. The checkpoint option should come from arguments.
        if inherit_checkpoint_context(None):
            # Checkpoint initial actor state at creation.
            store = workflow_storage.get_workflow_storage()
            store.save_physical_actor_class_body(_actor_id, self._metadata.cls)
            store.save_physical_actor_state(_actor_id, 0, (args, kwargs))
        return self._create(_actor_id, _ray_options, *args, **kwargs)

    def _create(self, actor_id, ray_options, *args, **kwargs):
        if ray_options is not None:
            ray_actor = self._ray_actor.options(**ray_options)
        else:
            ray_actor = self._ray_actor
        actor_handle = ray_actor.remote(*args, **kwargs)
        return WorkflowActor(actor_handle, self._metadata, actor_id=actor_id)

    # TODO(suquark): support num_cpu etc in options
    def options(self, **ray_options: Dict[str, Any]) -> WorkflowActorClassBase:
        """Configures and overrides the actor instantiation parameters."""
        actor_cls = self

        class ActorOptionWrapper(WorkflowActorClassBase):
            def create(self, *args, **kwargs):
                return actor_cls.create(
                    *args, _ray_options=ray_options, **kwargs)

        return ActorOptionWrapper()

    def __reduce__(self):
        return workflow_actor, (self._metadata.cls, )


class WorkflowActor(WorkflowActorBase):
    """The instance of a workflow actor class."""

    def __init__(self, actor_handle: "ray.actor.ActorHandle",
                 metadata: ActorMetadata, actor_id):
        self._actor_handle = actor_handle
        self._metadata = metadata
        self._actor_id = actor_id
        self._state_index = 0

    # We use methods instead of properties because __getattr__ cannot work
    # well with property.
    def ray_actor_handle(self) -> "ray.actor.ActorHandle":
        return self._actor_handle

    def actor_id(self) -> str:
        """The actor ID of the actor."""
        return self._actor_id

    def state_index(self) -> int:
        return self._state_index

    def __getattr__(self, method_name):
        if method_name in self._metadata.methods:
            return ActorMethod(
                self, self._metadata.cls, method_name, runtime_options=None)
        raise AttributeError(f"No method with name '{method_name}'")

    def __reduce__(self):
        raise ValueError(
            "WorkflowActor objects are not serializable. "
            "This means they cannot be passed or returned from Ray "
            "remote, or stored in Ray objects. It also cannot be included in "
            "the return value of a workflow step.")
        # TODO(suquark): enable serialziation of workflow actor later?
        # return WorkflowActor, (self._actor_handle, self._metadata,
        #                        self._storage, self._actor_id)


def workflow_actor(cls):
    """Decorate and convert a class to virtual actor class."""
    return WorkflowActorClass._from_class(cls)
