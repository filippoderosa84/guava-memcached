### Guava Memcached

A Guava cached implementation backed by memcached. Guava cache library provides an implementation for in memory cache.
This Guava Memcached provides an implementation of Guava Cache backed by memcached.

The memcached client implementation used here is a modified version (provided by AWS labs) of the Spymemcached library. 
This modified version is an enhanced version that allows to connect to AWS ElastiCache clusters
and to use autodiscovery feature, to allow automatic discovery of nodes within a cluster.

Details can be found here: https://github.com/awslabs/aws-elasticache-cluster-client-memcached-for-java

##### How to use

TBC