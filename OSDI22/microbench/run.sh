#! /bin/bash

CUR_DIR='readlink -f ./'
LOG_DIR=data
LOG_PATH=../$LOG_DIR

function pipelineTest()
{
	PIPELINE_RESULT=$1
	echo $PIPELINE_RESULT
	NUM_STAGES=1
	test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
	echo "base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
	for w in 1 2 4 8 
	do
		for o in 1000000000 4000000000 8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
				python test_pipeline.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}
function remainTest()
{
	PIPELINE_RESULT=$1
	echo $PIPELINE_RESULT
	NUM_STAGES=1
	test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
	echo "base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
	for w in 8 
	do
		for o in 8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
				python test_pipeline.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
				rm -rf /tmp/ray/session_2*
			done
		done
	done
}

pushd ../
mkdir -p $LOG_DIR
popd

# Pipeline Test
#./../script/install_scheduler_ray.sh
#pipelineTest $LOG_PATH/pipeline_memory.csv
#./../script/install_production_ray.sh
#pipelineTest $LOG_PATH/pipeline_production.csv

pipelineTest $LOG_PATH/pipeline_memory_blockEvictTasks_stage5.csv
