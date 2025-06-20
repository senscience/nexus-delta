# Elasticsearch Pipes

Pipes are the processing units of a pipeline for an Elasticsearch view.

See @ref:[here](./elasticsearch-view-api.md#processing-pipeline) to get more details on how pipes are applied and how the 
indexing process to Elasticsearch works. 

## Core pipes

These pipes are provided by default by Delta.

### Filter deprecated

* Allows excluding deprecated resources from being indexed
* No config is needed

```json
{
  "name" : "filterDeprecated"
}
```

### Filter by type

* Allow excluding resources which don't have one of the provided types

```json
{
  "name" : "filterByType",
  "config" : {
    "types" : [
      "https://bluebrain.github.io/nexus/types/Type1",
      "https://bluebrain.github.io/nexus/types/Type2"
    ]
  }
}
```

### Filter by schema

* Allow excluding resources which haven't been validated by one of the provided schemas

```json
{
  "name" : "filterBySchema",
  "config" : {
    "types" : [
      "https://bluebrain.github.io/nexus/schemas/Schema1",
      "https://bluebrain.github.io/nexus/schemas/Schema2"
    ]
  }
}
```

### Discard metadata

* Prevents all Nexus metadata from being indexed
* No configuration is needed

```json
{
  "name" : "discardMetadata"
}
```

### Source as text

* The original payload of the resource will be stored in the ElasticSearch document as a single
  escaped string value under the key `_original_source`.
* No configuration is needed

```json
{
  "name" : "sourceAsText"
}
```

### Data construct query

* The data graph of the resource will be transformed according to the provided SPARQL construct query
* The resource metadata is not modified by this pipe

```json
{
  "name" : "dataConstructQuery",
  "config": {
    "query": "{constructQuery}"
  }
}
```

### Select predicates

* Only the defined predicates in the data graph of the resource will be kept in the resource
* The resource metadata is not modified by this type

```json
{
  "name" : "selectPredicates",
  "config": {
    "predicates": [
      "rdfs:label",
      "schema:name"
    ]
  }
}
```

### Default label predicates

* Only default labels defined as `skos:prefLabel`, `rdf:tpe`, `rdfs:label`, `schema:name` will be kept in the data graph of the resource
* No configuration is needed

```json
{
  "name" : "defaultLabelPredicates"
}
```
