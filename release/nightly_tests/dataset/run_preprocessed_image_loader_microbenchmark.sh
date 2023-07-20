#!/bin/bash


DIR="/tmp/imagenet-1gb"
MOSAIC_DIR="/tmp/mosaicml-data"
TFRECORDS_DIR="/tmp/tf-data"

rm -rf DIR
rm -rf MOSAIC_DIR
rm -rf TFRECORDS_DIR


# Download 1GB dataset from S3 to local disk so we can preprocess with mosaic.
aws s3 sync s3://air-cuj-imagenet-1gb $DIR

python preprocess_tfrecords.py --data-root "$DIR" --tf-data-root "$TFRECORDS_DIR"

python preprocessed_image_loader_microbenchmark.py --data-root $DIR --mosaic-data-root "$MOSAIC_DIR" --tf-data-root "$TFRECORDS_DIR"
