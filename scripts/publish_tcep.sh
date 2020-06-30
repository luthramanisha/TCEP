#!/usr/bin/env bash
# Author: Manisha Luthra
# Modified by: Sebastian Hennig
# Description: Sets up and execute TCEP on GENI testbed

work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"
source "$work_dir/docker-swarm.cfg"
source "$work_dir/scripts/common_functions.sh"
if [ -z $2 ]; then
  u=$USER
else
  u=$2
fi
if [ -z $3 ]; then
  host=$manager
else
  host=$3
fi
token="undefined"
maki_pat='^[10]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
geni_pat='^[72,130]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
cl_pat='^[128]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
u=""

setUser() {

#if [[ $1 =~ $maki_pat ]]; then
#	u=$maki_user
#elif [[ $1 =~ $geni_pat ]]; then
#	u=$geni_user
#else
#	u=$cl_user
#fi
  u=$user
}

all() {
    adjust_cfg
    ./build.sh
    #setup
    START=$(date +%s.%N)
    take_down_swarm && \
    swarm_token=$(init_manager) && \
    join_workers $swarm_token && \
    PUB_START=$(date +%s.%N) && \
    publish && \
    END=$(date +%s.%N) && \
    TOT_DIFF=$(echo "$END - $START" | bc) && \
    echo "total time diff is $TOT_DIFF" && \
    PUB_DIFF=$(echo "$END - $PUB_START" | bc) && \
    echo "publish time diff is $PUB_DIFF"
}

adjust_cfg() {
    local sections=$n_speed_streams
    local nNodesTotal=$((${#workers[@]}+1))
    adjust_config $sections $sections $nNodesTotal $host "false" "false"
}

take_down_swarm() {
    setUser $manager
    echo "Taking down possible existing swarm with manager $manager"
    ssh -Tq -p $port $u@$manager 'docker stack rm tcep; docker swarm leave --force && docker network prune -f || exit 0;'
    for i in "${workers[@]}"
    do
	      setUser $i
	      echo "$i leaving swarm"
        ssh -Tq -p $port $u@$i 'docker swarm leave --force && docker network prune -f || exit 0;'
    done
}

setup_instance() {
    echo "Setting up instance $1"

    setUser $1
    echo "logged in $u"
    ssh-keyscan -H $1 >> ~/.ssh/known_hosts
    ssh -t -p $port $u@$1 "sudo hostname node$2"
    #ssh -T -p $port $u@$1 "grep -q -F '127.0.0.1 $2' /etc/hosts || sudo bash -c \"echo '127.0.0.1 $2' >> /etc/hosts\""
    ssh -T -p $port $u@$1 <<-'ENDSSH'
        mkdir -p ~/src && mkdir -p ~/logs

    if ! [ -x "$(command -v docker)" ]; then
        # Update the apt package index
        sudo apt-get update

        # Install packages to allow apt to use a repository over HTTPS
        sudo apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        software-properties-common

        # Add Docker’s official GPG key
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

        # Use the following command to set up the stable repository
        sudo add-apt-repository \
        "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) \
        stable"

        # Update the apt package index
        sudo apt-get update

        # Install the latest version of Docker CE
        sudo apt-get install docker-ce -y

        # Create the docker group.
        sudo groupadd docker

        # Add your user to the docker group.
        sudo usermod -aG docker $USER

	#Install docker-compose version 1.17
	sudo curl -L https://github.com/docker/compose/releases/download/1.17.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
	sudo chmod +x /usr/local/bin/docker-compose

    else
        echo "Docker already installed on $1"
        sudo usermod -a -G docker $USER
    fi
ENDSSH
}

setup() {
  if [ ${#workers[@]} -le $((n_publisher_nodes_total)) ]; then
      echo "not enough non-manager nodes (currently ${#workers[@]}) for ${n_publisher_nodes_total} publishers and at least 1 worker app" && exit 1
  else
    echo "$(date +%H:%M:%S) setting up manager $manager and workers $workers"
    count=0
    for i in "${workers[@]}"
    do
      count=$((count+1))
      setUser $i
      setup_instance $i $count #> /dev/null
    done
    setUser $manager
    setup_instance $manager 0
    res=`wait` # wait for all setup_instance calls to finish
    echo "$(date +%H:%M:%S) setup done"
  fi
}

init_manager() {
    
     setUser $manager
    `ssh -T -p $port $u@$manager "sudo hostname node0; sudo systemctl restart docker"` 2> /dev/null
    `ssh -T -p $port $u@$manager "docker swarm init --advertise-addr $manager"` 2> /dev/null
    # currently we are pushing jar file to all of the workers and building image locally on all of the workers
    # in future we should use docker registry service to create image on manager and use it as reference in worker nodes
    #ssh $user@$manager "docker service create --name registry --publish published=5000,target=5000 registry:2"
    token=`ssh -T -p $port $u@$manager 'docker swarm join-token worker -q'`
    echo $token #using this a global variable (DONT REMOVE)
}

join_workers() {
    count=0
    for i in "${workers[@]}"
    do
        setUser $i
	    count=$((count+1))
        ssh $u@$i "echo 'processing worker $i'"
        ssh -T -p $port $u@$i "docker swarm join --token $1 $manager:2377"
        #ssh -T -p $port $u@$i "sudo systemctl restart docker" 2> /dev/null
        # add labels to docker nodes so replicas can be deployed according to label
        if [ "$count" -le "$n_publisher_nodes_total" ]; then
          ssh -T -p $port $user@$manager "docker node update --label-add publisher=true node$count"
          echo "added label publisher=true to node$count "
        else
          ssh -T -p $port $user@$manager "docker node update --label-add worker=true node$count"
          echo "added label worker=true to node$count "
        fi
    done
    ssh -T -p $port $u@$manager "docker node update --label-add subscriber=true node0"
    echo "added label subscriber=true to node0"
}


# get the output from the manager node
# Usage bash publish_tcep.sh getOutput
get_output(){
	setUser $manager	
	mkdir -p $work_dir/logs_backup/$(date '+%d-%b-%Y-%H-%M-%S')
	scp -r $u@$manager:~/logs/* $work_dir/logs_backup/$(date '+%d-%b-%Y-%H-%M-%S')
}

clear_logs() {
    setUser $manager
    ssh $u@$manager "rm -f ~/logs/*.log && rm -f ~/logs/*.csv" &
    for i in "${workers[@]}"
    do
	    setUser $i
      ssh $u@$i "rm -f ~/logs/*.log && rm -f ~/logs/*.csv" &
    done

}

publish() {
    get_output
    clear_logs
    setUser $manager
    printf "\nPulling image from registry\n"
    ssh -T -p $port $user@$manager "docker pull $registry_user/$tcep_image"
    rm -rf $work_dir/dockerbuild
    ssh -T -p $port $user@$manager "docker pull $registry_user/$gui_image"

    # stop already existing services
    ssh $user@$manager 'docker service rm $(docker service ls -q)'

    echo "Booting up new stack"
    ssh -p $port $u@$manager 'mkdir -p ~/logs && rm -f ~/logs/** && mkdir -p ~/src';
    scp -P $port $work_dir/docker-stack.yml $u@$manager:~/src/docker-stack.yml
    #ssh -p $port $u@$manager 'cd ~/src && docker stack deploy --prune --with-registry-auth -c docker-stack.yml tcep';
    ssh -p $port $u@$manager 'cd ~/src && docker stack deploy --with-registry-auth -c docker-stack.yml tcep';
    #clear_logs
}

rebootSwarm() {
    ##take_down_swarm
    for i in "${workers[@]}"
    do
	      setUser $i
        echo "rebooting worker $i"
        ssh -p $port $u@$i "sudo reboot" &>/dev/null
    done
    echo "rebooting manager"
    setUser $manager
    ssh -p $port $u@$manager "sudo reboot" &>/dev/null
}


# Set the port variable default to 22 if not set
if [ -z $port ];
then port=22
fi

help="
Invalid usage

Publish TCEP script

Usage: ./publish_tcep.sh <COMMAND>

Available COMMAND options:
setup: Installs docker resources on every node
publish: Publish the application on the cluster and run it
take_down: Delete docker swarm cluster
all: Run all steps to publish the cluster and start the application
"

if [ -z $1 ]; then
    echo "$help"
    exit 1
fi

if [ $1 == "publish" ]; then publish
elif [ $1 == "setup" ]; then setup
elif [ $1 == "all" ]; then all
elif [ $1 == "take_down" ]; then take_down_swarm
elif [ $1 == "init_manager" ]; then init_manager
elif [ $1 == "reboot" ]; then rebootSwarm
elif [ $1 == "build_remote" ]; then build_remote
elif [ $1 == "get_output" ]; then get_output
else $1
fi
