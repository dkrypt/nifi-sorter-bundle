# nifi-sorter-bundle
This is a working sample custom processor, which sorts a given list of numbers as flowfile in NiFi.

This project is part of a blog post [here]()
## Developer Guide

Run:
```shell
mvn clean install
````

Copy `nifi-sorter-bundle/nifi-sorter-nar/target/nifi-sorter-nar-1.0.0.nar` to `lib` directory of your NiFi binary.

Run NiFi `./nifi.sh start`