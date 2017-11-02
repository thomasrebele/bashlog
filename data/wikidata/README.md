This repository provides two experiments. For both of them you need to download a Wikidata truthy dump:
https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-truthy.nt.gz
The one from October 27th, 2017 contains 2.1G triples.

## People

This experiments extracts a few relation from Wikidata centered on people and places and then provides a complex TBox and a few queries on them.
To generate the relation files you should run "python3 build_wikidata_people.py latest-truthy.nt.gz".

The TBox is in people_tbox.txt and the queries in people_queries.txt


## Full

This experiments works on the full dump. You have to run "python3 build_simple_wikidata_dump.py latest-truthy.nt.gz" to get the preprocessed dump as a TSV file.

The TBox contained in full_tbox.txt is much simpler and we provide 3 queries in full_queries.txt:
* query3: simple filter
* query4: required to compute a closure
* query5: join and more complex closure
