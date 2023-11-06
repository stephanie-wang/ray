import gc
import os
import platform
import tracemalloc
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type, Union

import numpy as np

from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.episode_v2 import EpisodeV2
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
    PublicAPI,
)
from ray.rllib.utils.deprecation import Deprecated, deprecation_warning
from ray.rllib.utils.exploration.random_encoder import (
    _MovingMeanStd,
    compute_states_entropy,
    update_beta,
)
from ray.rllib.utils.typing import AgentID, EnvType, PolicyID
from ray.tune.callback import _CallbackMeta

# Import psutil after ray so the packaged version is used.
import psutil

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm import Algorithm
    from ray.rllib.evaluation import RolloutWorker, WorkerSet


@PublicAPI
class DefaultCallbacks(metaclass=_CallbackMeta):
    """Abstract base class for RLlib callbacks (similar to Keras callbacks).

    These callbacks can be used for custom metrics and custom postprocessing.

    By default, all of these callbacks are no-ops. To configure custom training
    callbacks, subclass DefaultCallbacks and then set
    {"callbacks": YourCallbacksClass} in the algo config.
    """

    def __init__(self, legacy_callbacks_dict: Dict[str, callable] = None):
        if legacy_callbacks_dict:
            deprecation_warning(
                "callbacks dict interface",
                (
                    "a class extending rllib.algorithms.callbacks.DefaultCallbacks; see"
                    " `rllib/examples/custom_metrics_and_callbacks.py` for an example."
                ),
                error=True,
            )

    @OverrideToImplementCustomLogic
    def on_algorithm_init(
        self,
        *,
        algorithm: "Algorithm",
        **kwargs,
    ) -> None:
        """Callback run when a new Algorithm instance has finished setup.

        This method gets called at the end of Algorithm.setup() after all
        the initialization is done, and before actually training starts.

        Args:
            algorithm: Reference to the Algorithm instance.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_workers_recreated(
        self,
        *,
        algorithm: "Algorithm",
        worker_set: "WorkerSet",
        worker_ids: List[int],
        is_evaluation: bool,
        **kwargs,
    ) -> None:
        """Callback run after one or more workers have been recreated.

        You can access (and change) the worker(s) in question via the following code
        snippet inside your custom override of this method:

        Note that any "worker" inside the algorithm's `self.worker` and
        `self.evaluation_workers` WorkerSets are instances of a subclass of EnvRunner.

        .. testcode::
            from ray.rllib.algorithms.callbacks import DefaultCallbacks

            class MyCallbacks(DefaultCallbacks):
                def on_workers_recreated(
                    self,
                    *,
                    algorithm,
                    worker_set,
                    worker_ids,
                    is_evaluation,
                    **kwargs,
                ):
                    # Define what you would like to do on the recreated
                    # workers:
                    def func(w):
                        # Here, we just set some arbitrary property to 1.
                        if is_evaluation:
                            w._custom_property_for_evaluation = 1
                        else:
                            w._custom_property_for_training = 1

                    # Use the `foreach_workers` method of the worker set and
                    # only loop through those worker IDs that have been restarted.
                    # Note that we set `local_worker=False` to NOT include it (local
                    # workers are never recreated; if they fail, the entire Algorithm
                    # fails).
                    worker_set.foreach_worker(
                        func,
                        remote_worker_ids=worker_ids,
                        local_worker=False,
                    )

        Args:
            algorithm: Reference to the Algorithm instance.
            worker_set: The WorkerSet object in which the workers in question reside.
                You can use a `worker_set.foreach_worker(remote_worker_ids=...,
                local_worker=False)` method call to execute custom
                code on the recreated (remote) workers. Note that the local worker is
                never recreated as a failure of this would also crash the Algorithm.
            worker_ids: The list of (remote) worker IDs that have been recreated.
            is_evaluation: Whether `worker_set` is the evaluation WorkerSet (located
                in `Algorithm.evaluation_workers`) or not.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_checkpoint_loaded(
        self,
        *,
        algorithm: "Algorithm",
        **kwargs,
    ) -> None:
        """Callback run when an Algorithm has loaded a new state from a checkpoint.

        This method gets called at the end of `Algorithm.load_checkpoint()`.

        Args:
            algorithm: Reference to the Algorithm instance.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_create_policy(self, *, policy_id: PolicyID, policy: Policy) -> None:
        """Callback run whenever a new policy is added to an algorithm.

        Args:
            policy_id: ID of the newly created policy.
            policy: The policy just created.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_sub_environment_created(
        self,
        *,
        worker: "RolloutWorker",
        sub_environment: EnvType,
        env_context: EnvContext,
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Callback run when a new sub-environment has been created.

        This method gets called after each sub-environment (usually a
        gym.Env) has been created, validated (RLlib built-in validation
        + possible custom validation function implemented by overriding
        `Algorithm.validate_env()`), wrapped (e.g. video-wrapper), and seeded.

        Args:
            worker: Reference to the current rollout worker.
            sub_environment: The sub-environment instance that has been
                created. This is usually a gym.Env object.
            env_context: The `EnvContext` object that has been passed to
                the env's constructor.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_created(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        env_index: int,
        episode: Union[Episode, EpisodeV2],
        **kwargs,
    ) -> None:
        """Callback run when a new episode is created (but has not started yet!).

        This method gets called after a new Episode(V2) instance is created to
        start a new episode. This happens before the respective sub-environment's
        (usually a gym.Env) `reset()` is called by RLlib.

        1) Episode(V2) created: This callback fires.
        2) Respective sub-environment (gym.Env) is `reset()`.
        3) Callback `on_episode_start` is fired.
        4) Stepping through sub-environment/episode commences.

        Args:
            worker: Reference to the current rollout worker.
            base_env: BaseEnv running the episode. The underlying
                sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            policies: Mapping of policy id to policy objects. In single
                agent mode there will only be a single "default" policy.
            env_index: The index of the sub-environment that is about to be reset
                (within the vector of sub-environments of the BaseEnv).
            episode: The newly created episode. This is the one that will be started
                with the upcoming reset. Only after the reset call, the
                `on_episode_start` event will be triggered.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_start(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Union[Episode, EpisodeV2],
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Callback run right after an Episode has started.

        This method gets called after the Episode(V2)'s respective sub-environment's
        (usually a gym.Env) `reset()` is called by RLlib.

        1) Episode(V2) created: Triggers callback `on_episode_created`.
        2) Respective sub-environment (gym.Env) is `reset()`.
        3) Episode(V2) starts: This callback fires.
        4) Stepping through sub-environment/episode commences.

        Args:
            worker: Reference to the current rollout worker.
            base_env: BaseEnv running the episode. The underlying
                sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            policies: Mapping of policy id to policy objects. In single
                agent mode there will only be a single "default" policy.
            episode: Episode object which contains the episode's
                state. You can use the `episode.user_data` dict to store
                temporary data, and `episode.custom_metrics` to store custom
                metrics for the episode.
            env_index: The index of the sub-environment that started the episode
                (within the vector of sub-environments of the BaseEnv).
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_step(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        episode: Union[Episode, EpisodeV2],
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Runs on each episode step.

        Args:
            worker: Reference to the current rollout worker.
            base_env: BaseEnv running the episode. The underlying
                sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            policies: Mapping of policy id to policy objects.
                In single agent mode there will only be a single
                "default_policy".
            episode: Episode object which contains episode
                state. You can use the `episode.user_data` dict to store
                temporary data, and `episode.custom_metrics` to store custom
                metrics for the episode.
            env_index: The index of the sub-environment that stepped the episode
                (within the vector of sub-environments of the BaseEnv).
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Union[Episode, EpisodeV2, Exception],
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Runs when an episode is done.

        Args:
            worker: Reference to the current rollout worker.
            base_env: BaseEnv running the episode. The underlying
                sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            policies: Mapping of policy id to policy
                objects. In single agent mode there will only be a single
                "default_policy".
            episode: Episode object which contains episode
                state. You can use the `episode.user_data` dict to store
                temporary data, and `episode.custom_metrics` to store custom
                metrics for the episode.
                In case of environment failures, episode may also be an Exception
                that gets thrown from the environment before the episode finishes.
                Users of this callback may then handle these error cases properly
                with their custom logics.
            env_index: The index of the sub-environment that ended the episode
                (within the vector of sub-environments of the BaseEnv).
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_evaluate_start(
        self,
        *,
        algorithm: "Algorithm",
        **kwargs,
    ) -> None:
        """Callback before evaluation starts.

        This method gets called at the beginning of Algorithm.evaluate().

        Args:
            algorithm: Reference to the algorithm instance.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_evaluate_end(
        self,
        *,
        algorithm: "Algorithm",
        evaluation_metrics: dict,
        **kwargs,
    ) -> None:
        """Runs when the evaluation is done.

        Runs at the end of Algorithm.evaluate().

        Args:
            algorithm: Reference to the algorithm instance.
            evaluation_metrics: Results dict to be returned from algorithm.evaluate().
                You can mutate this object to add additional metrics.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_postprocess_trajectory(
        self,
        *,
        worker: "RolloutWorker",
        episode: Episode,
        agent_id: AgentID,
        policy_id: PolicyID,
        policies: Dict[PolicyID, Policy],
        postprocessed_batch: SampleBatch,
        original_batches: Dict[AgentID, Tuple[Policy, SampleBatch]],
        **kwargs,
    ) -> None:
        """Called immediately after a policy's postprocess_fn is called.

        You can use this callback to do additional postprocessing for a policy,
        including looking at the trajectory data of other agents in multi-agent
        settings.

        Args:
            worker: Reference to the current rollout worker.
            episode: Episode object.
            agent_id: Id of the current agent.
            policy_id: Id of the current policy for the agent.
            policies: Mapping of policy id to policy objects. In single
                agent mode there will only be a single "default_policy".
            postprocessed_batch: The postprocessed sample batch
                for this agent. You can mutate this object to apply your own
                trajectory postprocessing.
            original_batches: Mapping of agents to their unpostprocessed
                trajectory data. You should not mutate this object.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_sample_end(
        self, *, worker: "RolloutWorker", samples: SampleBatch, **kwargs
    ) -> None:
        """Called at the end of RolloutWorker.sample().

        Args:
            worker: Reference to the current rollout worker.
            samples: Batch to be returned. You can mutate this
                object to modify the samples generated.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_learn_on_batch(
        self, *, policy: Policy, train_batch: SampleBatch, result: dict, **kwargs
    ) -> None:
        """Called at the beginning of Policy.learn_on_batch().

        Note: This is called before 0-padding via
        `pad_batch_to_sequences_of_same_size`.

        Also note, SampleBatch.INFOS column will not be available on
        train_batch within this callback if framework is tf1, due to
        the fact that tf1 static graph would mistake it as part of the
        input dict if present.
        It is available though, for tf2 and torch frameworks.

        Args:
            policy: Reference to the current Policy object.
            train_batch: SampleBatch to be trained on. You can
                mutate this object to modify the samples generated.
            result: A results dict to add custom metrics to.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_train_result(
        self,
        *,
        algorithm: "Algorithm",
        result: dict,
        **kwargs,
    ) -> None:
        """Called at the end of Algorithm.train().

        Args:
            algorithm: Current Algorithm instance.
            result: Dict of results returned from Algorithm.train() call.
                You can mutate this object to add additional metrics.
            kwargs: Forward compatibility placeholder.
        """
        pass


class MemoryTrackingCallbacks(DefaultCallbacks):
    """MemoryTrackingCallbacks can be used to trace and track memory usage
    in rollout workers.

    The Memory Tracking Callbacks uses tracemalloc and psutil to track
    python allocations during rollouts,
    in training or evaluation.

    The tracking data is logged to the custom_metrics of an episode and
    can therefore be viewed in tensorboard
    (or in WandB etc..)

    Add MemoryTrackingCallbacks callback to the tune config
    e.g. { ...'callbacks': MemoryTrackingCallbacks ...}

    Note:
        This class is meant for debugging and should not be used
        in production code as tracemalloc incurs
        a significant slowdown in execution speed.
    """

    def __init__(self):
        super().__init__()

        # Will track the top 10 lines where memory is allocated
        tracemalloc.start(10)

    @override(DefaultCallbacks)
    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Union[Episode, EpisodeV2, Exception],
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        gc.collect()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        for stat in top_stats[:10]:
            count = stat.count
            # Convert total size from Bytes to KiB.
            size = stat.size / 1024

            trace = str(stat.traceback)

            episode.custom_metrics[f"tracemalloc/{trace}/size"] = size
            episode.custom_metrics[f"tracemalloc/{trace}/count"] = count

        process = psutil.Process(os.getpid())
        worker_rss = process.memory_info().rss
        worker_vms = process.memory_info().vms
        if platform.system() == "Linux":
            # This is only available on Linux
            worker_data = process.memory_info().data
            episode.custom_metrics["tracemalloc/worker/data"] = worker_data
        episode.custom_metrics["tracemalloc/worker/rss"] = worker_rss
        episode.custom_metrics["tracemalloc/worker/vms"] = worker_vms


def make_multi_callbacks(
    callback_class_list: List[Type[DefaultCallbacks]],
) -> DefaultCallbacks:
    """Allows combining multiple sub-callbacks into one new callbacks class.

    The resulting DefaultCallbacks will call all the sub-callbacks' callbacks
    when called.

    .. testcode::
        :skipif: True

        config.callbacks(make_multi_callbacks([
            MyCustomStatsCallbacks,
            MyCustomVideoCallbacks,
            MyCustomTraceCallbacks,
            ....
        ]))

    Args:
        callback_class_list: The list of sub-classes of DefaultCallbacks to
            be baked into the to-be-returned class. All of these sub-classes'
            implemented methods will be called in the given order.

    Returns:
        A DefaultCallbacks subclass that combines all the given sub-classes.
    """

    class _MultiCallbacks(DefaultCallbacks):
        IS_CALLBACK_CONTAINER = True

        def __init__(self):
            super().__init__()
            self._callback_list = [
                callback_class() for callback_class in callback_class_list
            ]

        @override(DefaultCallbacks)
        def on_algorithm_init(self, *, algorithm: "Algorithm", **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_algorithm_init(algorithm=algorithm, **kwargs)

        @override(DefaultCallbacks)
        def on_workers_recreated(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_workers_recreated(**kwargs)

        @override(DefaultCallbacks)
        def on_checkpoint_loaded(self, *, algorithm: "Algorithm", **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_checkpoint_loaded(algorithm=algorithm, **kwargs)

        @override(DefaultCallbacks)
        def on_create_policy(self, *, policy_id: PolicyID, policy: Policy) -> None:
            for callback in self._callback_list:
                callback.on_create_policy(policy_id=policy_id, policy=policy)

        @override(DefaultCallbacks)
        def on_sub_environment_created(
            self,
            *,
            worker: "RolloutWorker",
            sub_environment: EnvType,
            env_context: EnvContext,
            env_index: Optional[int] = None,
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_sub_environment_created(
                    worker=worker,
                    sub_environment=sub_environment,
                    env_context=env_context,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_episode_created(
            self,
            *,
            worker: "RolloutWorker",
            base_env: BaseEnv,
            policies: Dict[PolicyID, Policy],
            env_index: int,
            episode: Union[Episode, EpisodeV2],
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_episode_created(
                    worker=worker,
                    base_env=base_env,
                    policies=policies,
                    env_index=env_index,
                    episode=episode,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_episode_start(
            self,
            *,
            worker: "RolloutWorker",
            base_env: BaseEnv,
            policies: Dict[PolicyID, Policy],
            episode: Union[Episode, EpisodeV2],
            env_index: Optional[int] = None,
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_episode_start(
                    worker=worker,
                    base_env=base_env,
                    policies=policies,
                    episode=episode,
                    env_index=env_index,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_episode_step(
            self,
            *,
            worker: "RolloutWorker",
            base_env: BaseEnv,
            policies: Optional[Dict[PolicyID, Policy]] = None,
            episode: Union[Episode, EpisodeV2],
            env_index: Optional[int] = None,
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_episode_step(
                    worker=worker,
                    base_env=base_env,
                    policies=policies,
                    episode=episode,
                    env_index=env_index,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_episode_end(
            self,
            *,
            worker: "RolloutWorker",
            base_env: BaseEnv,
            policies: Dict[PolicyID, Policy],
            episode: Union[Episode, EpisodeV2, Exception],
            env_index: Optional[int] = None,
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_episode_end(
                    worker=worker,
                    base_env=base_env,
                    policies=policies,
                    episode=episode,
                    env_index=env_index,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_evaluate_start(
            self,
            *,
            algorithm: "Algorithm",
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_evaluate_start(
                    algorithm=algorithm,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_evaluate_end(
            self,
            *,
            algorithm: "Algorithm",
            evaluation_metrics: dict,
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_evaluate_end(
                    algorithm=algorithm,
                    evaluation_metrics=evaluation_metrics,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_postprocess_trajectory(
            self,
            *,
            worker: "RolloutWorker",
            episode: Episode,
            agent_id: AgentID,
            policy_id: PolicyID,
            policies: Dict[PolicyID, Policy],
            postprocessed_batch: SampleBatch,
            original_batches: Dict[AgentID, Tuple[Policy, SampleBatch]],
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_postprocess_trajectory(
                    worker=worker,
                    episode=episode,
                    agent_id=agent_id,
                    policy_id=policy_id,
                    policies=policies,
                    postprocessed_batch=postprocessed_batch,
                    original_batches=original_batches,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_sample_end(
            self, *, worker: "RolloutWorker", samples: SampleBatch, **kwargs
        ) -> None:
            for callback in self._callback_list:
                callback.on_sample_end(worker=worker, samples=samples, **kwargs)

        @override(DefaultCallbacks)
        def on_learn_on_batch(
            self, *, policy: Policy, train_batch: SampleBatch, result: dict, **kwargs
        ) -> None:
            for callback in self._callback_list:
                callback.on_learn_on_batch(
                    policy=policy, train_batch=train_batch, result=result, **kwargs
                )

        @override(DefaultCallbacks)
        def on_train_result(self, *, algorithm=None, result: dict, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_train_result(algorithm=algorithm, result=result, **kwargs)

    return _MultiCallbacks


# This Callback is used by the RE3 exploration strategy.
# See rllib/examples/re3_exploration.py for details.
class RE3UpdateCallbacks(DefaultCallbacks):
    """Update input callbacks to mutate batch with states entropy rewards."""

    _step = 0

    def __init__(
        self,
        *args,
        embeds_dim: int = 128,
        k_nn: int = 50,
        beta: float = 0.1,
        rho: float = 0.0001,
        beta_schedule: str = "constant",
        **kwargs,
    ):
        self.embeds_dim = embeds_dim
        self.k_nn = k_nn
        self.beta = beta
        self.rho = rho
        self.beta_schedule = beta_schedule
        self._rms = _MovingMeanStd()
        super().__init__(*args, **kwargs)

    @override(DefaultCallbacks)
    def on_learn_on_batch(
        self,
        *,
        policy: Policy,
        train_batch: SampleBatch,
        result: dict,
        **kwargs,
    ):
        super().on_learn_on_batch(
            policy=policy, train_batch=train_batch, result=result, **kwargs
        )
        states_entropy = compute_states_entropy(
            train_batch[SampleBatch.OBS_EMBEDS], self.embeds_dim, self.k_nn
        )
        states_entropy = update_beta(
            self.beta_schedule, self.beta, self.rho, RE3UpdateCallbacks._step
        ) * np.reshape(
            self._rms(states_entropy),
            train_batch[SampleBatch.OBS_EMBEDS].shape[:-1],
        )
        train_batch[SampleBatch.REWARDS] = (
            train_batch[SampleBatch.REWARDS] + states_entropy
        )
        if Postprocessing.ADVANTAGES in train_batch:
            train_batch[Postprocessing.ADVANTAGES] = (
                train_batch[Postprocessing.ADVANTAGES] + states_entropy
            )
            train_batch[Postprocessing.VALUE_TARGETS] = (
                train_batch[Postprocessing.VALUE_TARGETS] + states_entropy
            )

    @override(DefaultCallbacks)
    def on_train_result(self, *, result: dict, algorithm=None, **kwargs) -> None:
        # TODO(gjoliver): Remove explicit _step tracking and pass
        #  Algorithm._iteration as a parameter to on_learn_on_batch() call.
        RE3UpdateCallbacks._step = result["training_iteration"]
        super().on_train_result(algorithm=algorithm, result=result, **kwargs)


@Deprecated(
    new="ray.rllib.algorithms.callbacks.make_multi_callback([list of "
    "`Callbacks` sub-classes to combine into one])",
    error=True,
)
class MultiCallbacks(DefaultCallbacks):
    pass
