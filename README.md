#Aggregator

##Aggregation based on hashing

The idea here is to explore aggregation based on hashing the group by keys & using properties of
aggregate functions to arrive at the aggregates
####Properties of aggregate functions
* Sum, Min, Max, Count-- can be computed without storing the data points[Memory less]
* Average -- can be computed without storing the data points but cannot be merged[non-associative]
* Median & Quantiles -- need all the data points
* Quantiles can also be computed using Tdigest/QDigest [http://www.inf.fu-berlin.de/lehre/WS11/Wireless/papers/AgrQdigest.pdf]
Details on Qdigest & Tdigest http://info.prelert.com/blog/q-digest-an-algorithm-for-computing-approximate-quantiles-on-a-collection-of-integers
Sketch algorithms can also be used to count cardinality[HyperLogLog] & Top-N[Count-MinSketch]

Using Disk based cache to handle large data sets
As the records are parsed the group by keys are digested[compressed] into  hashkey.

* Key Cache -- maintains the hashkey and the details of the GenericGroupByKey
* Data Cache -- maintains the aggregate data [either all datapoints or only summary] for each group by combination

MurMur Hash [Guava Hashing.murmur3_128]  is used to generate hash for the groupby combination
Infinispan cache can be used to to spill to disk when overflowing the data; this will take care of large aggregation of large datasets

TODO: performance test with large data file
Running 
java -jar target/aggregator-jar-with-dependencies.jar <rawfactfile> <outdir>
 Example:
java -jar target/aggregator-jar-with-dependencies.jar /tmp/fact2.csv /tmp/aggdata/