#!/usr/bin/env bash
work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"

user=$USER
mininet_sim_vms=("10.2.1.40" "10.2.1.42" "10.2.1.64")
controller_vms=("10.2.1.15" "10.2.1.44" "10.2.1.48")

eval_time=100
queries=('Stream' 'Filter' 'Conjunction' 'Disjunction' 'Join' 'SelfJoin' 'AccidentDetection')
mapeks=('lightweight' 'requirementBased')
# requirement to change to after half the simulation duration
requirements=('load' 'hops')

setup_swarm(){
    bash publish_tcep.sh all ${user} ${manager}
}

swarm_sim() {
for mapek in ${mapeks[@]}
do
  for req in ${requirements[@]}
  do
    for q in ${queries[@]}
    do
        python set_properties.py ${mapek} ${req} ${q} 12
        for i in {1..5}
        do
          echo "$(date '+%H-%M-%S' ) starting simulation run $i with $q $req $mapek"
          bash publish_tcep.sh publish ${user} ${manager}
          sleep 4m
        done
    done
  done
done
}

mininet_sim() {
    local query_part=$1
    local controller=$2
    local host=$3
    for mapek in ${mapeks[@]}
    do
      for req in ${requirements[@]}
      do
        for q in ${queries[@]}
        do
            for i in {1..5}
            do
              # output is retrieved by publish_mininet-wifi.sh script
              echo "$(date '+%H-%M-%S') starting simulation run $i on $host with $q $req $mapek and controller on $controller"
              ./publish_mininet-wifi.sh run -u ${user} -h ${host} -c ${controller} -d 2 -a Relaxation -m ${mapek} -q ${q} --req ${req} &>/dev/null
            done
        done
      done
    done
}

mininet_a() {
    q_part_a=('AccidentDetection' 'Stream' 'Filter')
    mininet_sim ${q_part_a} "10.2.1.44" "localhost"
}

mininet_b() {
    q_part_b=('Conjunction' 'Disjunction')
    mininet_sim ${q_part_b} "10.2.1.15" "localhost"
}

mininet_c() {
    q_part_c=('Join' 'SelfJoin')
    mininet_sim ${q_part_b} "10.2.1.64" "localhost"
}

mininet_all() {
    echo "mininet_all args: $1 $2"
    mininet_sim ${queries} $1 $2
}
#
# do not call this with nohup, only directly since you must enter password
start_controller() {
    local host=$1
    local controller=$2
    pushd mininetSim/mininet-wifi
    echo "calling start_controller with $host $controller"
    ./publish_mininet-wifi.sh start_controller -u ${user} -h ${host} -c ${controller}
    popd
}

configure_passwordless_sudo() {
    local host=$1
    #line="$user ALL=(ALL:ALL) NOPASSWD: /usr/bin/python2.7, /usr/bin/mn, /usr/local/bin/mn /bin/cat /usr/sbin/visudo"
    line="$user ALL=(ALL:ALL) NOPASSWD: ALL"
    if [[ -z $(ssh ${user}@${host} "sudo cat /etc/sudoers | grep $line ") ]]; then
        echo "add the following line to the end of /etc/sudoers of $host via visudo: "
        echo ${line}
        ssh -t ${host} "sudo visudo"
    fi
}

setup_all() {
    [[ ${#mininet_sim_vms[@]} -gt ${#controller_vms[@]} ]] && echo "not enough controller_vms declared, exiting" && exit 1
    for i in "${!mininet_sim_vms[@]}"
    do
        local host=${mininet_sim_vms[$i]}
        local controller=${controller_vms[$i]}
        configure_passwordless_sudo ${host}
        ${work_dir}/_archive/copyKeys.sh ${host}
        pushd mininetSim/mininet-wifi/
        ./publish_mininet-wifi.sh compile -u ${user} -h ${host} -c ${controller}
        ./publish_mininet-wifi.sh setup  -u ${user} -h ${host} -c ${controller}
        popd
    done
}

clean_up() {
    for i in "${mininet_sim_vms[@]}"
    do
        ssh -t ${user}@${i} "sudo pkill -9 -f testTransitions.sh; sudo mn -c; sudo pkill -9 -f tcep sudo pkill -9 -f publish_mininet-wifi.sh "
    done
}

# call this to deploy the mininet simulations to all specified remote VMs
# detaches after starting simulations, so it is safe to close connection to VMs without affecting simulation execution
# requirements:
# - user must be in /etc/sudoers on each of the mininet_sim_vms see configure_passwordless_sudo()
# - mininet-wifi must be set up via setup_all()
deploy_all() {
    clean_up
    [[ ${#mininet_sim_vms[@]} -gt ${#controller_vms[@]} ]] && echo "not enough controller_vms declared, exiting" && exit 1
    for i in "${!mininet_sim_vms[@]}"
    do
        local host=${mininet_sim_vms[$i]}
        local controller=${controller_vms[$i]}
	      echo "deploying host $host with controller $controller"
        scp testTransitions.sh mininetSim/mininet-wifi/wifi-sumo-simulation.py mininetSim/mininet-wifi/publish_mininet-wifi.sh common_functions.sh ${work_dir}/docker-swarm.cfg ${USER}@${host}:~/tcep/mininet-wifi/
        ssh ${user}@${host} "sed -i -r 's#\)\/\.\.\/\.\.\/\.\.#)#' ~/tcep/mininet-wifi/publish_mininet-wifi.sh"
        ssh ${user}@${host} "sed -i -r 's#\/scripts\/#/#' ~/tcep/mininet-wifi/publish_mininet-wifi.sh"
        start_controller ${host} ${controller}
        # this does not work as intended, must call nohup ... manually on vm
        #ssh ${user}@${host} "nohup ~/tcep/mininet-wifi/testTransitions.sh mininet_all ${controller} ${host} &"
        echo "now run 'cd tcep/mininet-wifi && nohup ./testTransitions.sh mininet_all ${controller} ${host} &' on each host"
    done
}

delete_mininet-wifi_folders() {
    for i in "${!mininet_sim_vms[@]}"
    do
        local host=${mininet_sim_vms[$i]}
        line="~/tcep/mininet-wifi ~/tcep/mininet-wifi/hostap ~/netbee ~/wmediumd ~/ofsoftswitch13 ~/mac80211_hwsim_mgmt"
        echo "removing folders on $host $line"
        ssh -t ${user}@${host} "sudo rm -rfd $line"
    done
}

filter_logs(){
  for i in "${mininet_sim_vms[@]}"
  do
    for q in ${queries[@]}
    do
      ssh ${user}@${i} "cd tcep/logs_backup && mkdir $q && find . -type f -name 'MFGS*-$q*.csv' -exec cp {} $q/ \;"
    done
    ssh ${user}@${i} "cd tcep/logs_backup && mkdir lightweightTimings && find . -type f -name '*Lightweight*.csv' -exec cp {} lightweightTimings/ \;"
  done
}

CMD=$1
shift
${CMD} $@