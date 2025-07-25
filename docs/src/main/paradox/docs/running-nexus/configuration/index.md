# Nexus configuration

Nexus Delta service can be highly customized using @link:[configuration file(s)](https://github.com/senscience/nexus-delta/tree/$git.branch$/delta/app/src/main/resources){ open=new }. Many things can be adapted to your deployment needs: port where the service is running, timeouts, pagination defaults, etc. 

There are 3 ways to modify the default configuration:

- Setting the env variable `DELTA_EXTERNAL_CONF` which defines the path to a HOCON file. The configuration keys that are defined here can be overridden by the other methods.
- Using JVM properties as arguments when running the service: -D`{property}`. For example: `-Dapp.http.interface="127.0.0.1"`.
- Using @link:[FORCE_CONFIG_{property}](https://github.com/lightbend/config#optional-system-or-env-variable-overrides){ open=new }
  environment variables. In order to enable this style of configuration, the JVM property
  `-Dconfig.override_with_env_vars=true` needs to be set. Once set, a configuration flag can be overridden. For example: `CONFIG_FORCE_app_http_interface="127.0.0.1"`.

In terms of JVM pool memory allocation, we recommend setting the following values to the `JAVA_OPTS` environment variable: `-Xms4g -Xmx4g`. The recommended values should be changed accordingly with the usage of Nexus Delta, the number of projects and the resources/schemas size.

In order to successfully run Nexus Delta there is a minimum set of configuration flags that need to be specified

## Http configuration

@link:[The `http` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf#L11){ open=new } of the configuration defines the binding address and port where the service will be listening.

The configuration flag `akka.http.server.parsing.max-content-length` can be used to control the maximum payload size allowed for Nexus Delta resources. This value applies to all posted resources except for files.

## Postgres configuration

### Database pool
@link:[The `database` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf#L23){ open=new } of the configuration defines the postgres specific configuration. As Nexus Delta uses three separate pools ('read', 'write', 'streaming'), it is recommended to set the host, port, database name, username, and password via the `app.defaults.database` field, as it will apply to all pools. It is however possible to accommodate more advanced setups by configuring each pool separately by changing its respective `app.database.{read|write|streaming}` fields. 

The pool size can be set using the `app.defaults.database.access.pool-size` setting for all pools, or individually for each pool (`app.database.{read|write|streaming}.access.pool-size`).

@@@ note { .warning }

A default Postgres deployment will limit the number of connections to 100, unless configured otherwise. See the @link:[Postgres Connection and Authentication documentation](https://www.postgresql.org/docs/current/runtime-config-connection.html){ open=new }.

@@@

### Partitioning

Nexus supports currently two types of partitioning list and hash.

This can be set via the  `app.database.partition-strategy`.

Note that it is an essential setting which can not be changed automatically once it is set, so configure it carefully according to your needs.

More information about partitioning is available @ref:[here](../postgresql.md).

### Init scripts

Before running Nexus Delta, the @link:[init scripts](https://github.com/senscience/nexus-delta/tree/$git.branch$/delta/sourcing-psql/src/main/resources/scripts/postgres/init){ open=new } should be run in the lexicographical order.

The scripts in the subdirectory matching your partitioning strategy should be run first, followed by those in the common folder.

It is possible to let Nexus Delta automatically create them using the following configuration parameters: `app.database.tables-autocreate=true`.

@@@ note { .warning }

Auto creation of tables is included as a development convenience and should be avoided in production.

@@@

## Service account configuration

Nexus Delta uses a service account to perform automatic tasks under the hood. Examples of it are:

- Granting default ACLs to the user creating a project.
- Creating default views on project creation.

@link:[The `service-account` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf#L427){ open=new } of the configuration defines the service account configuration.

## Realm provisioning

Realm provisioning allows to create one or several realm at startup.

It is useful to start a new deployment with having to call the @ref:[realm API](../../delta/api/realms-api.md) to create those.

Exemple:
```hocon
realms {
    #...

    # To provision realms at startup
    # Only the name and the OpenId config url are mandatory
    provisioning {
      enabled = true
      realms {
         my-realm = {
          name = "My realm name"
          open-id-config = "https://.../path/.well-known/openid-configuration"
          logo = "https://senscience.ai/path/favicon.png"
          accepted-audiences = ["audience1", "audience2"]
        }
      }
    }
  }
```

@@@ note { .warning }

Realm provisioning will only create realms.
If a realm with the same identifier exists it will not be updated.

@@@

@link:[The `realms.provisioning` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf){ open=new } of the configuration defines it.

## Acl provisioning

Acl provisioning allows to create one or several acls at startup.

It is useful to start a new deployment with having to call the @ref:[ACL API](../../delta/api/acls-api.md) to create those.

Exemple:
```hocon
acls {
    #...

    # To provision acls at startup
    provisioning {
      enabled = true
      path = /path/to/initial/acl
    }
  }
```

where the file should be readable from Delta and follow the following json format similar to the ACL API:

```json
{
        "/" : [
          {
            "permissions": [
              "projects/read",
              "projects/write"
            ],
            "identity": {
              "realm": "realm",
              "group": "admins"
            }
          }
        ],
        "/org/proj": [
          {
            "permissions": [
              "resources/read",
              "resources/write"
            ],
            "identity": {
              "realm": "realm",
              "group": "org-proj-users"
            }
          }
        ]
      }
```
Like the 

@@@ note { .warning }

The realms and the permissions defined in the file must exist in the deployment.

Acl provisioning will only run if none is create at the root level.

@@@

@link:[The `acls.provisioning` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf){ open=new } of the configuration defines it.

### Elasticsearch configuration

The elasticsearch plugin configuration can be found @link:[here](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/elasticsearch/src/main/resources/elasticsearch.conf){ open=new }.

The most important flags are:

* `app.elasticsearch.base` which defines the endpoint where the Elasticsearch service is running.
* `app.elasticsearch.credentials.username` and `app.elasticsearch.credentials.password` to allow to access to a secured Elasticsearch cluster. The user provided should have the privileges to create/delete indices and read/index from them.

Please refer to the @link[Elasticsearch configuration](https://www.elastic.co/docs/deploy-manage/security) which describes the different steps to achieve this.


## Fusion configuration

When fetching a resource, Nexus Delta allows to return a redirection to its representation in Fusion by providing `text/html` in the `Accept` header.

@link:[The `fusion` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf#L40){ open=new } of the configuration defines the fusion configuration.

## Projections configuration

Projections in Nexus Delta are asynchronous processes that can replay the event log and process this information. For more information on projections, please refer to the @ref:[Architecture page](../../delta/architecture.md).

@link:[The `projections` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf#L281) of the configuration allows to configure the projections.

In case of failure in a projection, Nexus Delta records the failure information inside the `public.failed_elem_logs` PostgreSQL table, which can be used for analysis, and ultimately resolution of the failures. The configuration allows to set how long the failure information is stored for (`app.projections.failed-elem-ttl`), and how often the projection deleting the expired failures is awoken (`app.projections.delete-expired-every`).

## Plugins configuration

Since 1.5.0, Nexus Delta supports plugins. Jar files present inside the local directory defined by the `DELTA_PLUGINS` environment variable are loaded as plugins into the Delta service. 

Each plugin configuration is rooted under `plugins.{plugin_name}`. All plugins have a `plugins.{plugin_name}.priority` configuration flag used to determine the order in which the routes are handled in case of collisions. 

For more information about plugins, please refer to the @ref:[Plugins page](../../delta/plugins/index.md).

### Blazegraph views plugin configuration

The blazegraph plugin configuration can be found @link:[here](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/plugins/blazegraph/src/main/resources/blazegraph.conf){ open=new }. 

The most important flag is `plugins.blazegraph.base` which defines the endpoint where the Blazegraph service is running.

The @link:[`plugins.blazegraph.slow-queries` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/plugins/blazegraph/src/main/resources/blazegraph.conf#L23) of the Blazegraph configuration defines what is considered a slow Blazegraph query, which will get logged in the `public.blazegraph_queries` PostgreSQL table. This can be used to understand which Blazegraph queries can be improved.

### Composite views plugin configuration

The composite views plugin configuration can be found @link:[here](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/plugins/composite-views/src/main/resources/composite-views.conf){ open=new }. 

There are several configuration flags related to tweaking the range of values allowed for sources, projections and rebuild interval.

Authentication for remote sources can be specified in three different ways. The value of `plugins.composite-views.remote-source-credentials` can be set as:

##### Recommended: client credentials (OpenId authentication)
```hocon
{
  type: "client-credentials"
  user: "username"
  password: "password"
  realm: "internal"
}
```
This configuration tells Delta to log into the `internal` realm (which should have already been defined) with the `user` and `password` credentials, which will give Delta an access token to use when making requests to the remote instance

##### Anonymous
```hocon
{
  type: "anonymous"
}
```
##### Long-living auth token (legacy)
```hocon
{
  type: "jwt-token"
  token: "long-living-auth-token"
}
```

### Storage plugin configuration

The storage plugin configuration can be found @link:[here](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/plugins/storage/src/main/resources/storage.conf){ open=new }. 

Nexus Delta supports 2 types of storages: 'disk', 'amazon' (s3 compatible).

- For disk storages the most relevant configuration flag is `plugins.storage.storages.disk.default-volume`, which defines the default location in the Nexus Delta filesystem where the files using that storage are going to be saved.
- For S3 compatible storages the most relevant configuration flags are the ones related to the S3 settings: `plugins.storage.storages.amazon.default-endpoint`, `plugins.storage.storages.amazon.default-access-key` and `plugins.storage.storages.amazon.default-secret-key`.

#### File configuration

When the media type is not provided by the user, Delta relies on automatic detection based on the file extension in order to provide one.

From 1.9, it is possible to provide a list of extensions with an associated media type to compute the media type.

This list can be defined at `files.media-type-detector.extensions`:
```hocon
files {
  # Allows to define default media types for the given file extensions
  media-type-detector {
    extensions {
      custom = "application/custom"
      ntriples = "application/n-triples"
    }
  }
}
```

The media type resolution process follow this order stopping at the first successful step:

* Select the `Content-Type` header from the file creation/update request
* Compare the extension to the custom list provided in the configuratio
* Fallback on http4s automatic detection
* Fallback to the default value `application/octet-stream`

#### S3 storage configuration

Delta's S3 storage integration supports users uploading files to S3 independently and then registering them within Delta.

However, Delta is still responsible for the structure of the bucket so it issues a path to clients via the delegation validation endpoint (TODO link). This involves signing and later verifying a payload using JWS.

To support this functionality Delta must be configured with an RSA private key:
```hocon
      amazon {
        enabled = true
        default-endpoint = "http://s3.localhost.localstack.cloud:4566"
        default-access-key = "MY_ACCESS_KEY"
        default-secret-key = "CHUTCHUT"
        default-bucket = "mydefaultbucket"
        prefix = "myprefix"
        delegation {
            private-key = "${rsa-private-key-new-lines-removed}"
            token-duration = "3 days"
        }
      }
```

To generate such a key in the correct format follow these steps:
1. Generate RSA key: `openssl genrsa -out private_key.pem 2048`
2. Convert to PKCS#8 format: `openssl pkcs8 -topk8 -inform PEM -outform PEM -in private_key.pem -out private_key_pkcs8.pem -nocrypt`
3. Remove line breaks, copy secret: `cat private_key_pkcs8.pem | tr -d '\n' | pbcopy`

### Archive plugin configuration

The archive plugin configuration can be found @link:[here](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/plugins/archive/src/main/resources/archive.conf){ open=new }.

## Monitoring

### Kamon

For monitoring, Nexus Delta relies on @link:[Kamon](https://kamon.io/){ open=new }.

Kamon can be disabled by passing the environment variable `KAMON_ENABLED` set to `false`.

Delta configuration for Kamon is provided @link:[in the `monitoring` section](https://github.com/senscience/nexus-delta/blob/$git.branch$/delta/app/src/main/resources/app.conf#L391){ open=new }.
For a more complete description on the different options available, please look at the Kamon website.

#### Instrumentation

Delta provides the Kamon instrumentation for:

* @link:[Executors](https://kamon.io/docs/v1/instrumentation/executors/){ open=new }
* @link:[Scala futures](https://kamon.io/docs/v1/instrumentation/futures/){ open=new }
* @link:[Logback](https://kamon.io/docs/v1/instrumentation/logback/){ open=new }
* @link:[System metrics](https://kamon.io/docs/v1/instrumentation/system-metrics/){ open=new }

#### Reporters

Kamon reporters are also available for:

* @link:[Jaeger](https://kamon.io/docs/v1/reporters/jaeger/){ open=new }
* @link:[Prometheus](https://kamon.io/docs/v1/reporters/prometheus/){ open=new }

### OpenTelemetry

Since 1.12, Nexus also allows to use OpenTelemetry via otel4s where some configuration tips are provided 
@link:[here](https://typelevel.org/otel4s/sdk/configuration.html).

OpenTelemetry is disabled by default and can be enabled by setting the system property `otel.sdk.disabled` or
the environment variable `OTEL_SDK_DISABLED` to false.

When enabled, the otlp endpoint must also be provided via the system property `otel.exporter.otlp.endpoint` 
or the environment variable `OTEL_EXPORTER_OTLP_ENDPOINT`.

Note the default protocol inherited by the OpenTelemetry SDK is `grpc`.

#### Logs

For logs, Nexus relies on the OpenTelemetry Log SDK and the logback appender.

See @link:[here](https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/instrumentation/logback/logback-appender-1.0/library/README.md) for the configuration options.

The appender can be configured via the logback.xml file.

```xml
<appender name="OpenTelemetry"
    class="io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender">
</appender>
```