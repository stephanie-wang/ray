To run a condition, first edit cluster.yaml:
  - You need to update the setup commands to be sure you're either building from source or install 0.7.7 (see comments in setup commands section).
  - You also need to verify that the {head,worker} start commands have the correct flags.

Run ray up -y cluster.yaml - this should be fast to recompile if you don't tear down the cluster/install ray 0.7.7.

When running scripts, **be sure to do `source activate tensorflow_p36 before running`** and use the following environment variables to set conditions:

RAY_0_7=1 if running on ray==0.7.7
BY_VAL_ONLY=1 to always pass by value
CENTRALIZED=1 to use a "centralized owner"

