# FAQ

## General FAQ

### What is SENSCIENCE Nexus?

SENSCIENCE Nexus is an ecosystem that allows you to organize and better leverage your data through the use of a 
Knowledge Graph.

### Is SENSCIENCE Nexus free to use?

Yes, Nexus is a free, Open Source platform released under @link:[Apache Licence 2.0](https://www.apache.org/licenses/LICENSE-2.0){ open=new }

### How do I run Nexus?

Meanwhile if you want to run it locally you can do so using @ref:[Docker](running-nexus/index.md#docker). You can also deploy Nexus 
@ref:[“on premise”](running-nexus/index.md#on-premise-cloud-deployment), as a single instance or as a cluster. 
Nexus has also been deployed and tested on AWS and Azure using @link:[Kubernetes](https://kubernetes.io/){ open=new }.

### What is the difference with a relational database like PostgreSQL?

Although Nexus can be used as a regular database, it's flexibility and feature set are well beyond that. 
Just to mention some of the Nexus features:

- Allows the user to define different constraints to different sets of data at runtime
- Provides automatic indexing into several indexers (currently ElasticSearch and Sparql), dealing with reindexing 
strategies, retries and progress
- Provides authentication
- Comes with a flexible and granular authorization mechanism
- Guarantees resources immutability, keeping track of a history of changes.

### Is there a limit on the number of resources Nexus can store?

Nexus leverages scalable open source technologies, therefore limitations and performance depends heavily on 
the deployment setup where Nexus is running.

### What is a Knowledge Graph?

A Knowledge Graph is a modern approach to enabling the interlinked representations of entities (real-world objects, 
activities or concepts). In order to find more information about Knowledge Graphs, please visit the section 
@ref:["Understanding the Knowledge Graph"](getting-started/understanding-knowledge-graphs.md)

Nexus employs a Knowledge Graph to enable validation, search, analysis and integration of data.

### How do I report a bug? Which support Nexus team provide?

There are several channels provided to address different issues:

- **Bug report**: If you have found a bug while using the Nexus ecosystem, please create an issue 
  @link:[here](https://github.com/senscience/nexus-delta/issues/new/choose){ open=new }.
- **Questions**: if you need support, we will be reachable through the @link:[Github Discussions](https://github.com/senscience/nexus-delta/discussions){ open=new }
- **Documentation**: Technical documentation and 'Quick Start' to Nexus related concepts can be found 
  @link:[here](https://bluebrainnexus.io/docs/){ open=new }
- **Feature request**: If there is a feature you would like to see in Nexus, please first consult the 
  @link:[list of open feature requests](https://github.com/senscience/nexus-delta/issues?q=is%3Aissue%20state%3Aopen%20label%3Aenhancement){ open=new }. 
  In case there isn't already one, please 
  @link:[open a feature request](https://github.com/senscience/nexus-delta/issues/new/choose){ open=new } describing 
  your feature with as much detail as possible.

## Technical FAQ  

### What are the clients I can use with SENSCIENCE Nexus? What are the requirements to run Nexus locally?

Nexus requires at least 2 CPUs and 8 GB of memory in total. You can increase the limits in Docker settings in the menu 
_Preferences > Advanced_. More details are in the dedicated @ref:[page](running-nexus/index.md).

### What is JSON-LD?

JSON-LD is a JavaScript Object Notation for Linked Data. A JSON-LD payload is then converted to an RDF Graph for 
validation purposes and for ingestion in the Knowledge Graph. In order to find more information about JSON-LD, please 
visit this page, please visit the section @ref:[this page](getting-started/understanding-knowledge-graphs.md#json-ld)

### How can I represent lists on JSON-LD?

Using JSON-LD, arrays are interpreted as Sets by default. If you want an array to be interpreted as a list, you will 
have to add the proper context for it. For example, if the field containing the array is called `myfield`, then the 
context to be added would be:

```json
{
  "@context": {
    "myfield": {
      "@container": "@list"
    }
  }
}
```

You can find more information about Sets and Lists in JSON-LD on the 
@link:[Json-LD 1.1 specification](https://www.w3.org/TR/json-ld11/#lists-and-sets){ open=new }

### What is RDF?

The Resource Description Framework (RDF) is a graph-based data model used for representing information in the Web. 
The basic structure of any expression in RDF is in triples, an extremely easy segmentation of any kind of knowledge 
in subject-predicate-object. It is a family of W3C specifications, and was originally designed as a metadata model. 
In order to find more information about RDF and JSON-LD, please visit this page, please visit the section 
@ref:[this page](getting-started/understanding-knowledge-graphs.md#rdf)

### What is Elasticsearch?

@link:[Elasticsearch](https://www.elastic.co/elastic-stack/){ open=new } is a document oriented search engine with an 
HTTP endpoint and schema-free JSON document. It is able to aggregate data based on specific queries enabling the 
exploration of trends and patterns.

### What is a SHACL schema?

@link:[SHACL](https://www.w3.org/TR/shacl/){ open=new } (Shapes Constraint Language) is a language for validating RDF 
graphs against a set of conditions. These conditions are provided as shapes and other constructs expressed in the form 
of an RDF graph. SHACL is used in Nexus to constrain and control the payload that can be pushed into Nexus.
You can use the @link:[SHACL Playground](https://shacl.org/playground/){ open=new } to test your schemas.

### Do I need to define SHACL schemas to bring data in?

No. @link:[SHACL](https://www.w3.org/TR/shacl/){ open=new } schemas provide an extra layer of quality control for the 
data that is ingested into Nexus. However, we acknowledge the complexity of defining schemas. That's why clients can 
decide whether to use schemas to constrain their data or not, depending on their use case and their available resources.

### Where can I find SHACL shapes I can reuse (point to resources, like schema.org)?

@link:[Datashapes.org](https://datashapes.org/){ open=new } provides an automated conversion of 
@link:[schema.org](https://schema.org/){ open=new } as SHACL entities. A neuroscience community effort and INCF Special 
Interest Group - @link:[Neuroshapes](https://github.com/INCF/neuroshapes){ open=new }, provides open schemas for 
neuroscience data based on common use cases.

### Why are RDF and JSON-LD important for Nexus?

RDF is the data model used to ingest data into the Knowledge Graph and it is also used for SHACL schema data validation. 
JSON-LD is an RDF concrete syntax, and it is the main format we use for messages exchange. The choice of JSON-LD is due 
to the fact that is plain JSON but with some special 
@link:[keywords](https://www.w3.org/TR/json-ld11/#syntax-tokens-and-keywords){ open=new } and JSON is a broadly 
adopted API exchange format.

### Can I connect any SPARQL client to Nexus’ SPARQL endpoint?

Yes. As long as the client supports the ability to provide a `Authentication` HTTP Header (for authentication purposes) 
on the HTTP request, any SPARQL client should work.

### How can I create an organization as an anonymous user in the docker-compose file? What needs to be done to switch to "authenticated" mode?

By default, the permissions used - for an authenticated user - when running Nexus Delta are the ones defined on the JVM 
property @link:[app.permissions.minimum](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf#L155){ open=new }.
In order to change that behaviour, please create some ACLs for the path `/`. For more details about ACLs creation, 
visit the @ref:[ACLs page](delta/api/acls-api.md#create).

### Can I use Nexus from Jupyter Notebooks?

Nexus can be used from Jupyter Notebooks using 
@link:[Nexus Forge](https://github.com/blueBrain/nexus-forge){ open=new } or 
@link:[Nexus Python SDK](https://github.com/BlueBrain/nexus-python-sdk/){ open=new }. Alternatively, you can also use 
any Python HTTP client and use Nexus REST API directly from the Jupyter Notebook.
Other examples are provided in the folder @link:[Notebooks](https://github.com/BlueBrain/nexus-python-sdk/tree/master/notebooks){ open=new }.
