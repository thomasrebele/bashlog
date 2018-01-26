## bashlog

Download the Yago files `yagoFacts.tsv`, `yagoSimpleTaxonomy.tsv` and `yagoSimpleTypes.tsv`
The Tbox is in `full_tbox.txt` (update the path to the TSV files).
The queries are defined in `full_queries.txt`


## rdfox

Download the Yago files `yagoFacts.ttl`, `yagoSimpleTaxonomy.ttl` and `yagoSimpleTypes.ttl`
Merge `yagoSimpleTaxonomy.ttl` and `yagoSimpleTypes.ttl` in `yagoTypes.ttl`

Script to start all queries
```
for((i=1;i<=2;i++)); do
    rdfox() { 
        python3 ../../../experiments/competitors/rdfox/run-rdfox.py rdfox_tbox.dlog yagoTypes.ttl sparql/query$i.sparql > $tmp/yago-result$i-rdfox.txt;
	}
    run "rdfox: yago query $i" rdfox
done
for((i=3;i<=6;i++)); do
    rdfox() { 
        python3 ../../../experiments/competitors/rdfox/run-rdfox.py rdfox_tbox.dlog yagoFacts.ttl sparql/query$i.sparql > $tmp/yago-result$i-rdfox.txt;
	}
    run "rdfox: yago query $i" rdfox
done
```
