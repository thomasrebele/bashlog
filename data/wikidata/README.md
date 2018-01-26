This repository provides two experiments. For both of them you need to download a Wikidata truthy dump:
https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-truthy.nt.gz
The one from October 27th, 2017 contains 2.1G triples.

## Full

### Setup

To preprocess the dataset: `python3 build_simple_wikidata_dump.py latest-truthy.nt.gz`
(replace `latest-truthy.nt.gz` with the name of the downloaded dump if required).
It outputs a file called `all-triples.txt`.


### Execute queries

#### bashlog

The Tbox is in `full_tbox.txt` (update the path to the `all-triples.txt` file).
The queries are defined in `full_queries.txt`


### rdfox

Script to start all queries
```
for((i=1;i<=6;i++)); do
    rdfox() { 
        python3 ../../../experiments/competitors/rdfox/run-rdfox.py rdfox_tbox.dlog latest-truthy.nt.gz sparql/query$i.sparql > $tmp/wikidata-result$i-rdfox.txt;
	}
    run "rdfox: wikidata query $i" rdfox
done
```

## People

Outdated: do not run.

This experiments extracts a few relation from Wikidata centered on people and places and then provides a complex TBox and a few queries on them.
To generate the relation files you should run "python3 build_wikidata_people.py latest-truthy.nt.gz".

The TBox is in people_tbox.txt and the queries in people_queries.txt
