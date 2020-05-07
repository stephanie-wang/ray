#!/bin/bash

RAY_HEAD_IP=$1

echo "Reconnecting to head $RAY_HEAD_IP..."


source activate tensorflow_p36 && ray stop
pkill -9 ray || true
source activate tensorflow_p36 && RAY_GCS_SERVICE_ENABLED=false ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076 --internal-config="{\"record_ref_creation_sites\":0, \"initial_reconstruction_timeout_milliseconds\":100, \"num_heartbeats_timeout\":10, \"lineage_pinning_enabled\":1, \"free_objects_period_milliseconds\":-1, \"object_manager_repeated_push_delay_ms\":1000, \"task_retry_delay_ms\":1000}"
