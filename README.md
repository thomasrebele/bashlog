# bashlog
This tool translates datalog programs to Unix bash scripts. It can be used to preprocess large tabular datasets.
Please have a look at the [technical report](https://www.thomasrebele.org/publications/2018_report_bashlog.pdf) if you are interested how it works.
You can also try it [online](https://www.thomasrebele.org/projects/bashlog/).

## How to run it locally

1. Download [bashlog-datalog.jar](https://github.com/thomasrebele/bashlog/releases/download/v1.0/bashlog-datalog.jar) from the [releases](https://github.com/thomasrebele/bashlog/releases)
2. Write a datalog program ([examples](https://www.thomasrebele.org/projects/bashlog/datalog)).
3. Generate the script with `java -jar bashlog-datalog.jar --query-file <datalog-program> --query-pred <predicate>   > query.sh`
4. Execute it with `bash query.sh > result.txt`

## References

If you use bashlog in your research, please cite:

    @inproceedings{bashlog,
      author    = {Thomas Rebele and Thomas Pellissier Tanon and Fabian M. Suchanek},
      title     = {Bash Datalog: Answering Datalog Queries with Unix Shell Commands},
      booktitle = {International Semantic Web Conference},
      pages     = {566--582},
      year      = {2018}
    }
