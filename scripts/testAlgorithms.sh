#!/usr/bin/env bash

# 1: Relaxation
# 2: Starks
# 8: Rizou
# 9: Producer Consumer
# 10: Global Optimal
# 11: Random

eval_time=100
queries=('Stream' 'Conjunction' 'Disjunction' 'Join' 'SelfJoin' 'AccidentDetection')
#python set_algo.py 1
#bash publish_tcep.sh all
#sleep 3m

python set_algo.py 1 'Stream'
bash publish_tcep.sh all
sleep 4m

for q in ${queries[@]}
do
    python set_algo.py 1 ${q}
    for i in {1..5}
    do
        bash publish_tcep.sh publish
        sleep 3m
    done
done

for q in ${queries[@]}
do
    python set_algo.py 2 ${q}
    for i in {1..5}
    do
        bash publish_tcep.sh publish
        sleep 3m
    done
done

for q in ${queries[@]}
do
    python set_algo.py 8 ${q}
    for i in {1..5}
    do
        bash publish_tcep.sh publish
        sleep 3m
    done
done

for q in ${queries[@]}
do
    python set_algo.py 9 ${q}
    for i in {1..5}
    do
        bash publish_tcep.sh publish
        sleep 3m
    done
done

for q in ${queries[@]}
do
    python set_algo.py 10 ${q}
    for i in {1..3}
    do
        bash publish_tcep.sh publish
        sleep 3m
    done
done

for q in ${queries[@]}
do
    python set_algo.py 11 ${q}
    for i in {1..5}
    do
        bash publish_tcep.sh publish
        sleep 3m
    done
done