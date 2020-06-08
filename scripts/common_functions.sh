setup_docker_remote() {
  ssh $1 "docker --version" || ssh $1 -tt <<-'ENDSSH'
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

        # install missing kernelmodules and openvswitch since they can't be included with the container
        wget -O ~/tcep/install.sh https://raw.githubusercontent.com/containernet/containernet/master/util/install.sh
        chmod u+x ~/tcep/install.sh
        sudo ~/tcep/install.sh -v
    else
        echo "Docker already installed on $1"
        sudo usermod -a -G docker $USER
    fi
ENDSSH
}

setup_sumo() {
  ssh $1 "sumo-gui --version" || ssh $1 -tt 'sudo LC_ALL=C.UTF-8 add-apt-repository -y ppa:sumo/stable && \
    sudo apt-get update && \
    sudo apt-get install -y sumo sumo-tools'
}

adjust_config() {
    local sections=$1
    local nSpeedPublishers=$2
    local nNodesTotal=$3
    local gui=$4
    local mininet=$5
    local wifi=$6

    echo "configuring application.conf for $nSpeedPublishers speed publishers and $nNodesTotal nodes total"
    sed -i -r "s#mininet-simulation = .*#mininet-simulation = ${mininet}#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#min-nr-of-members = [0-9]*#min-nr-of-members = $nNodesTotal#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#number-of-speed-publisher-nodes = [0-9]*#number-of-speed-publisher-nodes = $nSpeedPublishers#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#number-of-road-sections = [0-9]*#number-of-road-sections = $sections#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#gui-endpoint = \"(.*?)\"#gui-endpoint = \"http://10.0.0.254:3000\"#" ${work_dir}/src/main/resources/application.conf
    sed -i -r 's| \"akka\.tcp://tcep@| #\"akka.tcp://tcep@|' ${work_dir}/src/main/resources/application.conf

    if [[ $mininet == "true" ]]; then
      if [[ $wifi == "true" ]]; then
        sed -i -r 's| #\"akka\.tcp://tcep@20\.0\.0\.15:\"\$\{\?constants\.base-port\}\"\"| \"akka.tcp://tcep@20.0.0.15:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
      else
        sed -i -r 's| #\"akka\.tcp://tcep@10\.0\.0\.253:\"\$\{\?constants\.base-port\}\"\"| \"akka.tcp://tcep@10.0.0.253:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
      fi
    else
      sed -i -r 's| #\"akka\.tcp://tcep@simulator:\"\$\{\?constants\.base-port\}\"\"| \"akka.tcp://tcep@simulator:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
    fi
    sed -i -r "s#const SERVER = \"(.*?)\"#const SERVER = \"${machine}\"#" ${work_dir}/gui/constants.js
    sed -i -r "s#const SERVER = \"(.*?)\"#const SERVER = \"${machine}\"#" ${work_dir}/gui/src/graph.js
}