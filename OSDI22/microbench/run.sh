#! /bin/bash

# Pipeline Test
	#Production Ray
	./../script/install_production_ray.sh
PIPELINE_RESULT=../data/pipeline_production.csv
NUM_STAGES=1
test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
echo "working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
for w in 1 2 4 8 16
do
	for o in 1000000000 5000000000 10000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
	do
		for ((os=10000000; os<=160000000; os *= 2))
		do
			echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
			python test_pipeline.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
			rm -rf /tmp/ray/session_2*
		done
	done
done
	#Memory Scheduled Ray
./../script/install_scheduler_ray.sh
PIPELINE_RESULT=../data/pipeline_memory.csv
NUM_STAGES=1
test -f "$PIPELINE_RESULT" && rm $PIPELINE_RESULT
echo "working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$PIPELINE_RESULT
for w in 1 2 4 8 16
do
	for o in 1000000000 5000000000 10000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
	do
		for ((os=10000000; os<=160000000; os *= 2))
		do
			echo -n -e "test_pipeline.py -w $w -o $o -os $os\n"
			python test_pipeline.py -w $w -o $o -os $os -r $PIPELINE_RESULT -ns $NUM_STAGES
			rm -rf /tmp/ray/session_2*
		done
	done
done
