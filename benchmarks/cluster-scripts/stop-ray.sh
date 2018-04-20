#!/bin/bash


parallel-ssh -i -h workers.txt -x "-o StrictHostKeyChecking=no" "export PATH=/home/ubuntu/anaconda3/bin/:$PATH && ray stop"
