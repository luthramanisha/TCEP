#!/usr/bin/env bash

work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"
source $work_dir/docker-swarm.cfg


maki_pat='^[10]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
geni_pat='^[72,130]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
cl_pat='^[128]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
u=$user
'''
if [[ $manager =~ $maki_pat ]]; then 
	u=$maki_user
elif [[ $manager =~ $geni_pat ]]; then
	u=$geni_user
else
	u=$cl_user
fi
'''



if [ -z $1 ]; then
	ssh $u@$manager
else
	'''
	if [[ ${workers[$1]} =~ $maki_pat ]]; then 
		u=$maki_user
	elif [[ ${workers[$1]} =~ $geni_pat ]]; then
		u=$geni_user
	else
		u=$cl_user
	fi
	'''
	ssh $u@${workers[$1]}
fi

