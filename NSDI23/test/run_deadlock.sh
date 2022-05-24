#! /bin/bash

echo 'RAY_record_ref_creation_sites=1 RAY_object_spilling_threshold=1.0 RAY_enable_BlockTasks=true RAY_enable_BlockTasksSpill=true python spill_time_measure.py'
RAY_record_ref_creation_sites=1 RAY_BACKEND_LOG_LEVEL=debug RAY_object_spilling_threshold=1.0 RAY_enable_BlockTasks=true RAY_enable_BlockTasksSpill=true python deadlock2.py 
