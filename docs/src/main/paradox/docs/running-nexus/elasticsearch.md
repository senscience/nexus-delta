# Elasticsearch

Nexus uses @link:[Elasticsearch](https://www.elastic.co/elasticsearch){ open=new } to host several _system_ indices and _user
defined_ ones. It offers sharding and replication out of the box. Deciding whether this system requires backup depends
on the tolerated time for a restore. Nexus can be instructed to rebuild all indices using the data from the _primary
store_, but being an incremental indexing process it can take longer than restoring from a backup. Since it can be
configured to host a number of replicas for each shard it can tolerate a number of node failures.

The Elasticsearch @link:[setup documentation](https://www.elastic.co/docs/deploy-manage/deploy/self-managed/installing-elasticsearch){ open=new }
contains the necessary information on how to install and configure it, but recommendations on sizing the nodes and
cluster are scarce because it depends on usage.

@link:[This section of the documentation](https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance) and @link:[the blog](https://www.elastic.co/blog) also gives useful recommendations on how to run Elasticsearch.

@@@ note { .info }
Nexus is currently tested with Elasticsearch 8.x but should still work with the 7.x series.

To benefit from the latest features and bug fixes from Elastic, we recommend you to stick as much as possible to the latest releases.
@@@

## Running and monitoring

**CPU:**
It suggests heavy indexing and search operations so:

* The indexing strategy can be reviewed by ajusting the batch
* The search queries can be optimized
* Elasticsearch may not be caching correctly
* More resources need to be allocated to Elasticsearch either by scaling vertically or horizontally

**Memory and garbage collection:**
Elasticsearch will use the available RAM in 2 ways, JVM heap and the file system cache
so the JVM garbage collection frequency and duration are also important to monitor.

**Storage:**
If less than 20% of the disk space is available on a disk node, any insert or update of it will fail.

Elasticsearch can also do a lot of reads and writes to disk so it is also a good it to keep an eye on I/O utilization.

**Cluster health:**
Elasticsearch provides a cluster health endpoint giving the status of the cluster (green/yellow/red).

This is important to monitor as it indicates in the yellow case that some data is likely to become unavailable
if more shards disappear.

When the status turns to red, a primary shard is missing. This prevents indexing on that shard and causes search
to return partial results.

**Number of shards per node:**
Every view in Nexus uses at least 2 shards (one for a primary and one for a replica) and
Elasticsearch allows a maximum number of shards per node.

So when the number of the projects and views grow, Elasticsearch may run out of available shards and any new project or
view creation will result in an error.

@@@ note { .info }
To solve this, an option can be to add another node on the cluster.

Another option if used with caution can be to raise the maximum number of shards per node.

This can be done via the elasticsearch configuration file:
```hocon
cluster.max_shards_per_node: 2000
```

Of via a call to the API:
```
PUT _cluster/settings
{
  "persistent": {
    "cluster.routing.allocation.total_shards_per_node": 2000
  }
}
```

@@@

**Search and indexing performance:**

Query load and latency must be monitored to make sure that information in a timely manner to the client.

On the write side, indexing load and latency must also be watched especially
if the data must be available for search as soon as possible.

## Tools and resources

Elasticsearch provide ways to monitor it via different api endpoints and via Kibana.

For further monitoring, there are several options with the Elastic stack itself but it requires another cluster dedicated
to monitoring and purchase a license to have access to some monitoring and alerting features.

Another option allowing to push data in Prometheus is
@link:[elasticsearch_exporter](https://github.com/prometheus-community/elasticsearch_exporter).
