#!/bin/bash

rm -rf *-logs
for worker in $(cat workers.txt); do
    mkdir $worker-logs
    scp -o StrictHostKeyChecking=no $worker:/tmp/raylogs/raylet_0* $worker-logs/
    scp -o StrictHostKeyChecking=no $worker:/tmp/raylogs/plasma_store* $worker-logs/
done
