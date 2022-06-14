from ray.util.iter import LocalIterator
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.typing import SampleBatchType
from ray.util.iter_metrics import MetricsContext

# Backward compatibility.
from ray.rllib.utils.metrics import (  # noqa: F401
    LAST_TARGET_UPDATE_TS,
    NUM_TARGET_UPDATES,
    APPLY_GRADS_TIMER,
    COMPUTE_GRADS_TIMER,
    WORKER_UPDATE_TIMER,
    GRAD_WAIT_TIMER,
    SAMPLE_TIMER,
    LEARN_ON_BATCH_TIMER,
    LOAD_BATCH_TIMER,
)

STEPS_SAMPLED_COUNTER = "num_steps_sampled"
AGENT_STEPS_SAMPLED_COUNTER = "num_agent_steps_sampled"
STEPS_TRAINED_COUNTER = "num_steps_trained"
STEPS_TRAINED_THIS_ITER_COUNTER = "num_steps_trained_this_iter"
AGENT_STEPS_TRAINED_COUNTER = "num_agent_steps_trained"

# End: Backward compatibility.


# Asserts that an object is a type of SampleBatch.
def _check_sample_batch_type(batch: SampleBatchType) -> None:
    if not isinstance(batch, (SampleBatch, MultiAgentBatch)):
        raise ValueError(
            "Expected either SampleBatch or MultiAgentBatch, "
            "got {}: {}".format(type(batch), batch)
        )


def _get_shared_metrics() -> MetricsContext:
    """Return shared metrics for the training workflow.

    This only applies if this algorithm has an execution plan."""
    return LocalIterator.get_metrics()
