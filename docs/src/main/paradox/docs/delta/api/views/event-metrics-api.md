# Event metrics

This global view is a single projection routine indexing event data for all projects.
While the other views focus on the state level (whether it is on a tagged or latest state), this view gives a
different perspective allowing to compute @ref:[project statistics](../projects-api.md#fetch-statistics) 
or @ref:[storage statistics](../storages-api.md#fetch-statistics).

While the underlying index is not directly exposed, it is possible to create dashboards with visualization tools like
Kibana to get information about the top contributors, the most frequents events, etc.

Like the other views, Nexus provide endpoints to check its progress and indexing errors.

## Fetch indexing

```
GET /v1/views/{org_label}/{project_label}/{view_id}/offset
```

**Example**

Request
:   @@snip [offset.sh](../assets/views/blazegraph/sparql/offset.sh)

Response
:   @@snip [offset.json](../assets/views/blazegraph/sparql/offset.json)

where...

- `instant` - timestamp of the last event processed by the view
- `value` - the value of the offset

## Fetch statistics

```
GET /v1/event-metrics/statistics
```

where...

- `totalEvents` - total number of events in the project
- `processedEvents` - number of events that have been considered by the view
- `remainingEvents` - number of events that remain to be considered by the view
- `discardedEvents` - number of events that have been discarded (were not evaluated due to filters, e.g. did not match schema, tag or type defined in the view)
- `evaluatedEvents` - number of events that have been used to update an index
- `lastEventDateTime` - timestamp of the last event in the project
- `lastProcessedEventDateTime` - timestamp of the last event processed by the view
- `delayInSeconds` - number of seconds between the last processed event timestamp and the last known event timestamp

## Restart indexing

This endpoint restarts the view indexing process. It does not delete the created indices but it overrides the resource
Document when going through the event log.

```
DELETE /v1/views/{org_label}/{project_label}/{view_id}/offset?from={offset}
```

### Parameter description

- `{from}`: Number - is the parameter that describes the offset to restart from; defaults to `0` and will
  reindexing everything.
