#!/usr/bin/env bash

work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"
source $work_dir/docker-swarm.cfg

# default port for SSH is 22
if [ -z $port ];
then port=22
fi

fetch_logs() {
    hostname=`ssh $user@$1 'hostname'`
    mkdir $output_dir/logs/logs-$hostname
    ssh -Tq -p $port $user@$1 "rm -rf ~/logs*.zip && cd ~/logs && zip -r -o ~/logs-$1.zip *"
    scp $user@$1:/users/$user/logs-$1.zip $output_dir/logs/
    unzip $output_dir/logs/logs-$1.zip -d $output_dir/logs/logs-$hostname
    rm $output_dir/logs/logs-$1.zip
}

rm -rfd $output_dir/logs
mkdir $output_dir/logs

if [ -z $1 ]; then
  for i in "${workers[@]}"
  do
      fetch_logs $i & 2> /dev/null
  done
  fetch_logs $manager
else
  fetch_logs $1
fi
