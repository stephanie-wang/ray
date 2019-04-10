#!/bin/bash

for host in $(cat ~/workers.txt); do
  ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R $host
  ssh -o "StrictHostKeyChecking no" $host 'uptime'
  if ! grep "$host$" ~/.ssh/config >1 /dev/null 2>&1; then
      echo "Host $host" >> ~/.ssh/config
      echo "    ForwardAgent yes" >> ~/.ssh/config
  fi
done

parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" -I < enable_hugepages.sh
