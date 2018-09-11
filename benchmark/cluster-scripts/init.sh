for host in $(cat workers.txt); do
  ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R $host
  ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $host 'uptime && echo "/home/ubuntu/core.%p" | sudo tee /proc/sys/kernel/core_pattern'
done

parallel-ssh -h workers.txt -O IdentityFile=~/devenv-key.pem -P -t 0 -I < ~/enable_hugepages.sh
parallel-ssh -h workers.txt -O IdentityFile=~/devenv-key.pem -P -t 0 "sudo apt-get install numactl"
