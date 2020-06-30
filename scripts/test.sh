queries=('Stream' 'Filter' 'Conjunction' 'Disjunction' 'Join' 'SelfJoin' 'AccidentDetection')

for q in ${queries[@]}
do
	python set_algo.py 1 ${q}
	for i in {1..5}
    	do
		echo "i: $i, q: $q"
	done
done 
