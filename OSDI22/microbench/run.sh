#! /bin/bash

LIST_LEN=1000
LIST_LEN_INCREASE=1000
LIST_LEN_MAX=10000

# Pipeline Test
for((list_len = $LIST_LEN; list_len <=$LIST_LEN_MAX; list_len += $LIST_LEN_INCREASE))
do
	echo -n -e "test_pipline -l $list_len\n"
	python test_pipeline.py -l $list_len
done
