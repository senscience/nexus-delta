# PostgreSQL

Nexus uses @link:[PostgreSQL](https://www.postgresql.org/){ open=new } as its _primary store_ as for its strong reputation for performance, reliability and flexibility.

Since this is the _primary store_ it is the most important system to be
@link:[backed up](https://www.postgresql.org/docs/current/backup.html){ open=new }. All of the data
that Nexus uses in other stores can be recomputed from the one stored in PostgreSQL as the other stores are used as
mere indexing systems.

### PostgreSQL partitioning

Nexus Delta takes advantage of PostgreSQL's @link:[Table Partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html) feature. This allows for improved query performance, and facilitates loading, deleting, or transferring data.

It currently supports 2 partition strategies offered by PostgreSQL:

* List partitioning
* Hash partitioning

@@@ note { .warning }

List partitioning is the default strategy.

Please be careful when configuring partitioning as Delta does not allow out-of-the box to adopt another one.
@@@

#### List partitioning

**This strategy is best when you have a small number of projects which can contain a lot of resources.**

The `public.scoped_events` and `public.scoped_states` are partitioned by organization, which is itself partitioned by the projects it contains; this follows the natural hierarchy that can be found in Nexus Delta.

Nexus Delta takes care of handling the creation and deletion of the partitions.

* If the created project is the first one of a given organization, both the organization partition and the project subpartition will be created.
* If the organization partition already exist, then only the project subpartition will be created upon project creation.

The naming scheme of the (sub)partitions is as follows:

`{table_name}_{MD5_org_hash}` for organization partitions

`{table_name}_{MD5_project_hash}` for project partition

where

* `{table_name}` is either `scoped_events` or `scoped_states`
* `{MD5_org_hash}` is the MD5 hash of the organization name
* `{MD5_project_hash}` is the MD5 hash of the project reference (i.e. has the form `{org_name}/{project_name}`)

MD5 hashing is used in order to guarantee a constant partition name length (PostgreSQL table names are limited to 63 character by default), as well as to avoid any special characters that might be allowed in project names but not in PostgreSQL table names (such as `-`).

Example:

You create the organization called `myorg`, inside of which you create the `myproject` project. When the project is created, Nexus Delta will have created the following partitions:

* `scoped_events_B665280652D01C4679777AFD9861170C`, the partition of events from the `myorg` organization
  * `scoped_events_7922DA7049D5E38C83053EE145B27596`, the subpartition of the events from the `myorg/myproject` project
* `scoped_states_B665280652D01C4679777AFD9861170C`, the partition of states from the `myorg` organization
  * `scoped_states_7922DA7049D5E38C83053EE145B27596`, the subpartition of the states from the `myorg/myproject` project

#### Hash partitioning

Since 1.12, Nexus allows hash partitioning. This strategy is best when you have a lot of projects containing a small amount of resources.

The `public.scoped_events` and `public.scoped_states` are partitioned with a fixed number of partitions where all events and states from a given project 
will end up in the same partition.

Nexus takes care of creating the different partitions the first time it starts with this configuration.

The naming scheme of the partitions is as follows:

- `{table_name}_{(%04d)}` (ex: `scoped_events_0001`)

#### Advanced subpartitioning

While Nexus Delta provides table partitioning out-of-the-box, it is primarily addressing the case where the data is more or less uniformly spread out across multiple projects. If however there is one or more project that are very large,
it is possible to add further subpartitions according to a custom rule. This custom subpartitioning must be decided on a case-by-case basis using your knowledge of the given project; the idea is to create uniform partitions of your project.

Please refer to the @link:[PostgreSQL Table Partitioning documentation](https://www.postgresql.org/docs/current/ddl-partitioning.html).

## Tables

### Initializing the schema

The different scripts are available @link:[here](https://github.com/senscience/nexus-delta/tree/master/delta/sourcing-psql/src/main/resources/scripts/postgres/init)
They must be run in the alphabetical order.

### Description

**global_events/global_states:**
Stores the events and the current states for entities which do not belong to a project (ex: realm, acl).

The events and states are structured as logs and can be ordered via the `ordering` column.

**scoped_events/scoped_states:**
Stores the events and the tagged and current states for entities which belong to a project (ex: resource, file 
and project itself).

The events and states are structured as logs and can be ordered via the `ordering` column.

The indexing routines rely heavily on the `scoped_states` table,

Those tables are partitioned as described in the @ref:[following section](#postgresql-partitioning).

**scoped_tombstones:**
This table allows to notify the indexing routines that a resource state has lost a property like a type or a tags so that they can
on their end delete the indexed resource from Elasticsearch for example.

The rows in this table are short-lived and are only as their notify purpose is temporary.

**ephemeral_states:**
Stores the states for entities which are immutable and short-lived (ex: archives).

**entity_dependencies:**
Stores the relationship of some entities in a project with entities living in other project preventing to delete projects
when other resources from other project still point to it (ex: cross-project resolver, aggregate views).

**projection_offsets:**
Stores the progress and statistics of the indexing processes related to the different views (except for composite views)
and some of the internal projections.

**composite_offsets:**
Stores the progress and statistics of the indexing processes related to composite views.

**projection_restarts:**
Allows to notify projections related to indexing views (except composite views) must be restarted.

The rows in this table are short-lived and are only as their notify purpose is temporary.

**composite_restarts:**
Same but for the composite views.

They are stored separately as more restart options are available for those views,

**failed_elem_logs:**
Stores errors which occured during the processing of some elements by projections (ex: a resource has been rejected by
Elasticsearch as it does not respect the mapping).

The rows in this table are short-lived and are only as their purpose is temporary.

**deleted_project_reports:**
Stores the result of project deletions as a report.

**blazegraph_queries:**
Stores slow queries submitted to Blazegraph.

The rows in this table are short-lived as they only have a monitoring purpose.

**scope_initialization_errors:**
Stores error related to project initialization where Nexus Delta executes a list of actions
such as creating a default Elasticsearch view.

Some of those actions may fail as it relies on external components (like a Blazegraph instance)
which may be down at this moment.

This table allows to save the failures in order to be able to a posteriori call 
@ref:[the supervision healing endpoint](../delta/api/supervision-api.md#project-healing).

**project_last_updates:**
Stores the last_update and last_ordering properties for the different projects.

This table allows to power the passivation strategy for the different views.

## Running and monitoring

**CPU:**
High CPU usage suggests inefficient query execution and query plans, resource contention, deadlocks.

**Memory:**
Low memory indicates swapping and degraded performance.

**Storage:**
There should be enough available space for PostgreSQL to operate properly.

As described in the @ref:[architecture section](../delta/architecture.md) the generally adopted
persistence model is an EventSourced model in which the data store is used as an _append only_ store. This has
implications to the total amount of disk used by the primary store.

**Locks:**
Locks can lead to high cpu usage and instability and are to be monitored and fixed.

**Read and write query throughput and performance:**
Helps to identify slow queries and potential issues with reading and writing data

**Active sessions:**
To avoid resource contention and to plan for scalability.

**Replication status and lag:**
To identify high availability and data consistency issues across replicated instances
High CPU and memory usage in one or several nodes can lead to increased replication lags

## Tools and resources

An approach to monitor PostgreSQL with Prometheus is to use Postgres exporter from the
@link:[Prometheus community](https://github.com/prometheus-community/postgres_exporter).

The PostgreSQL website also has a whole section about @link:[monitoring](https://www.postgresql.org/docs/current/monitoring.html) and the
@link:[pg_statstatements](https://www.postgresql.org/docs/current/pgstatstatements.html) allows to get statistics about the queries
executed by the server.