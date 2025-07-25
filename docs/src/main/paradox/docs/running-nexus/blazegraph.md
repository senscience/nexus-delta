# Blazegraph

Nexus uses @link:[Blazegraph](https://blazegraph.com/){ open=new } as an RDF (triple) store to provide a advanced querying
capabilities on the hosted data. This store is treated as a specialized index on the data so as with
Elasticsearch in case of failures, the system can be fully restored from the primary store. While the technology is
advertised to support @link:[High Availability](https://github.com/blazegraph/database/wiki/HAJournalServer){ open=new } and
@link:[Scaleout](https://github.com/blazegraph/database/wiki/ClusterGuide){ open=new } deployment configurations, we have yet to be able
to setup a deployment in this fashion.

Blazegraph can be deployed by:

* using the docker image created by BBP: https://hub.docker.com/r/bluebrain/blazegraph-nexus
* deploying Blazegraph using the prepackaged _tar.gz_ distribution available to download from
  @link:[GitHub](https://github.com/blazegraph/database/releases/tag/BLAZEGRAPH_2_1_6_RC){ open=new }.

@@@ note

We're looking at alternative technologies and possible application level (within Nexus) sharding and replicas.

Blazegraph is not actively maintained anymore since Amazon hired the development team for their Neptune product.
It makes operating it harder as resources about monitoring are scarce and bugs and issues about Blazegraph will remain
unsolved.

@@@

## Running and monitoring
**CPU:**
It suggests heavy indexing and query operations.
It is likely to happen when reindexing after updating search but if it happens regularly:

* Review the SPARQL queries made to Blazegraph
* Review the indexing strategy
* Allocate more resources to Blazegraph
* The Blazegraph and composite views plugin can point to different Blazegraph instances, this can be considered
  if you have a heavy usage of both plugins

**Memory and garbage collection:**
Blazegraph will use the available RAM in 2 ways, JVM heap and the file system cache
so like Elasticsearch, the JVM garbage collection frequency and duration are also important to monitor.

Using the supervision endpoints for @ref:[Blazegraph](../delta/api/supervision-api.md#blazegraph) 
and @ref:[composite views](../delta/api/supervision-api.md#composite-views) and 
the @link:[RAM guidance from Blazegraph](https://github.com/blazegraph/database/wiki/Hardware_Configuration#ram-sizing-guidance)
gives some insights on how much memory can be assigned.

**Storage:**

Blazegraph stores data in an append only journal which means updates will use additional disk space.

So the disk usage will grow with time depending on the rhythm of updates.

Compactions can be applied to the journal using the
@link:[CompactJournalUtility](https://github.com/blazegraph/database/blob/master/bigdata-core/bigdata/src/java/com/bigdata/journal/CompactJournalUtility.java){ open=new }
to reduce the disk usage, but it takes quite a bit a time and requires taking the software offline during the process.

An alternative can be to reindex on a fresh instance of Blazegraph, this approach also allows to reconfigure the
underlying namespaces.

Using the supervision endpoints for @ref:[Blazegraph](../delta/api/supervision-api.md#blazegraph)
and @ref:[composite views](../delta/api/supervision-api.md#composite-views) and
the @link:[Data on disk guidance from Blazegraph](https://github.com/blazegraph/database/wiki/Hardware_Configuration#data-on-disk-sizing-guidance)
gives some insights on how much storage can be assigned.

**Query and indexing performance:**

Query load and latency must be monitored to make sure that information in a timely manner to the client.

On the write side, indexing load and latency must also be watched especially
if the data must be available for search as soon as possible.

To help with this, Delta records long queries and stores them in the `blazegraph_queries` table and also leverage
@link:[Kamon tracing](https://kamon.io/docs/latest/core/tracing/) with spans to measure these operations
and if any of them fail.

## Tools and resources

The @link:[Hardware Configuration](https://github.com/blazegraph/database/wiki/Hardware_Configuration){ open=new } section in the
documentation gives a couple of hints about the requirements to operate Blazegraph and there are additional sections
for optimizations in terms of @link:[Performance](https://github.com/blazegraph/database/wiki/PerformanceOptimization){ open=new },
@link:[IO](https://github.com/blazegraph/database/wiki/IOOptimization){ open=new } and
@link:[Query](https://github.com/blazegraph/database/wiki/QueryOptimization){ open=new }.

The Nexus repository gives also:

* @link:[A jetty configuration](https://github.com/senscience/nexus-delta/blob/master/tests/docker/config/blazegraph/jetty.xml)
  allowing which allow to tune Blazegraph so as to handle better Nexus indexing/querying
* @link:[A log4j configuration](https://github.com/senscience/nexus-delta/blob/master/tests/docker/config/blazegraph/log4j.properties)
  As the default one provided by Blazegraph is insufficient
* @link:[The docker compose file](https://github.com/senscience/nexus-delta/blob/master/tests/docker/blazegraph.yml)
  for tests shows how to configure those files via system properties
* @link:[A python script](https://github.com/senscience/nexus-delta/blob/master/blazegraph/prometheus-exporter/prometheus-blazegraph-exporter.py)
  allowing to scrape Blazegraph metrics so as to push them to a Prometheus instance

## Backups

Pushing data to Blazegraph is time-consuming especially via the API as triples are verbose and Blazegraph 
does not allow large payloads.

So even if it is possible to repopulate a Blazegraph instance from the primary store, it is better to perform backup using
@link:[the online backup api endpoint](https://github.com/blazegraph/database/wiki/REST_API#online-backup).

@link:[Here](https://github.com/senscience/nexus-delta/blob/$git.branch$/kubernetes/blazegraph/backup-script.yaml) is an example of a backup script 
using this endpoint, compressing the resulting file and creating a checksum out of it.

The Nexus repository also provides:

* @link:[A Kubernetes cronjob to run it provided](https://github.com/senscience/nexus-delta/blob/$git.branch$/kubernetes/blazegraph/backup-cronjob.yaml).
* @link:[A Kubernetes cronjob allowing to delete old backups](https://github.com/senscience/nexus-delta/blob/$git.branch$/kubernetes/blazegraph/deleter-cronjob.yaml).

@@@ note

This script is meant to be run in Kubernetes as a cronjob but should be easily adaptable from other contexts by replacing the `kubectl` commands.

@@@

To restore a backup, the journal file related to the instance needs to be replaced by the one generated by the backup operation.