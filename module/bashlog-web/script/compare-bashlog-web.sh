#!/bin/bash

api=http://localhost:8080/bashlog-web/api

mkdir -p queries/data
(
	cd queries
	data_dir=$(realpath data)

	(
		cd $data_dir
		[ ! -e "sample.zip" ] && wget -N http://resources.mpi-inf.mpg.de/yago-naga/yago3.1/sample.zip && unzip sample.zip
		[ ! -e "sample-ntriples.zip" ] && wget -N http://resources.mpi-inf.mpg.de/yago-naga/yago3.1/sample-ntriples.zip && unzip sample-ntriples.zip
	)

	result_dir=$(realpath result)
	mkdir -p $result_dir

	for j in *.datalog; do
		i=$(basename $j .datalog)
		echo "running $i"

		# generate bash code
		# datalog query
		curl -s --data-binary @./$i.datalog $api/datalog\?query\=main\&debug_algebra\&debug_datalog > $result_dir/$i.bash-datalog
		# sparql/owl query
		curl -s --data-urlencode owl@./$i.owl --data-urlencode sparql@./$i.sparql --data-urlencode nTriples=yago-sample.ntriples $api/sparql?debug_algebra\&debug_datalog > $result_dir/$i.bash-sparql

		# run bash code
		(
			cd $data_dir
			bash $result_dir/$i.bash-datalog > $result_dir/$i.result-datalog
			bash $result_dir/$i.bash-sparql > $result_dir/$i.result-sparql
		)

		# check result
		c1=$(wc -l < $result_dir/$i.result-datalog)
		c2=$(wc -l < $result_dir/$i.result-sparql)

		if [ "$c1" == "$c2" ]; then
			
			echo "ok: $(printf '%-20s' "$i") produced $c1 rows"
		else
			echo ""
			echo "count $i datalog: $c1"
			echo "count $i sparql : $c2"
			echo ""
		fi

	done
)

