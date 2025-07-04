f# Files

Files are attachment resources rooted in the `/v1/files/{org_label}/{project_label}/` collection.

Each file belongs to a `project` identifier by the label `{project_label}` inside an `organization` identifier by the label `{org_label}`.


@@@ note { .tip title="Authorization notes" }	

When creating, updating and reading files, the caller must have the permissions defined on the storage associated to the file on the current 
path of the project or the ancestor paths.

Please visit @ref:[Authentication & authorization](authentication.md) section to learn more about it.

@@@

## Nexus metadata

When using the endpoints described on this page, the responses will contain global metadata described on the
@ref:[Nexus Metadata](../metadata.md) page. In addition, the following files specific metadata can be present

- `_bytes`: size of the file in bytes
- `_digest`: algorithm and checksum used for file integrity
- `_filename`: name of the file
- `_keywords`: list of keywords associated with the file and which can be used to search for the file
- `_location`: path where the file is stored on the underlying storage
- `_mediaType`: @link:[MIME](https://en.wikipedia.org/wiki/MIME){ open=new } specifying the type of the file
- `_origin`: whether the file attributes resulted from an action taken by a client or the Nexus Storage Service
- `_storage`: `@id`, `@type`, and revision of the @ref:[Storage](storages-api.md) used for the file
- `_uuid`: @link:[UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier){ open=new } of the file
- `_project`: address of the file's project

## Custom file metadata

When creating file resources, users can optionally provide custom metadata to be indexed and therefore searchable.

This takes the form of a `metadata` field containing a JSON Object with one or more of the following fields:

- `name`: a string which is a descriptive name for the file. It will be indexed in the full-text search.
- `description`: a string that describes the file. It will be indexed in the full-text search.
- `keywords`: a JSON object with `Label` keys and `string` values. These keywords will be indexed and can be used to search for the file.

## Indexing

All the API calls modifying a file (creation, update, tagging, deprecation) can specify whether the file should be indexed
synchronously or in the background. This behaviour is controlled using `indexing` query param, which can be one of two values:

- `async` - (default value) the file will be indexed asynchronously
- `sync` - the file will be indexed synchronously and the API call won't return until the indexing is finished

## Request body and headers for create and updates operations

**The request body:**

The body should be a multipart form, to allow file upload. The form should contain one part named `file`. This part can be given a content-type header, which will be used if specified. If not specified, the content-type will be inferred from the file's extension.

This part can contain the following disposition parameters:
- `filename`: the filename which will be used in the back-end file system

**Headers:**

- `x-nxs-file-metadata`: an optional JSON object containing one or more of fields described in @ref:[custom file metadata](#custom-file-metadata).
- `x-nxs-file-content-length`: the size of the uploaded file:
  - mandatory to upload to a S3 storage
  - ignored for other types of storage

## Create using POST

```
POST /v1/files/{org_label}/{project_label}?storage={storageId}&tag={tagName}
```

... where 

- `{storageId}` selects a specific storage backend where the file will be uploaded. This field is optional.
When not specified, the default storage of the project is used.
- `{tagName}` an optional label given to the file on its first revision.

@ref:[Request body and headers to provide](#request-body-and-headers-for-create-and-updates-operations)

**Example**

Request
:   @@snip [create-post.sh](assets/files/create-post.sh)

Response
:   @@snip [created-post.json](assets/files/created-post.json)

## Create using PUT

This alternative endpoint to create a resource is useful in case the json payload does not contain an `@id` but you want
to specify one. The @id will be specified in the last segment of the endpoint URI.

```
PUT /v1/files/{org_label}/{project_label}/{file_id}?storage={storageId}&tag={tagName}
```

... where 

- `{storageId}` selects a specific storage backend where the file will be uploaded. This field is optional. 
When not specified, the default storage of the project is used.
- `{tagName}` an optional label given to the file on its first revision.

@ref:[Request body and headers to provide](#request-body-and-headers-for-create-and-updates-operations)

**Example**

Request
:   @@snip [create-put.sh](assets/files/create-put.sh)

Response
:   @@snip [created-put.json](assets/files/created-put.json)

## Update

This operation overrides the file content.

In order to ensure a client does not perform any changes to a file without having had seen the previous revision of
the file, the last revision needs to be passed as a query parameter.

```
PUT /v1/files/{org_label}/{project_label}/{resource_id}?rev={previous_rev}
```

... where `{previous_rev}` is the last known revision number for the resource.

@ref:[Request body and headers to provide](#request-body-and-headers-for-create-and-updates-operations)

@@@ note { .tip title="Metadata update" }

If only the metadata is provided, then the updated is a metadata update and the file content is not changed.

@@@

**Example**

Request
:   @@snip [update.sh](assets/files/update.sh)

Response
:   @@snip [updated.json](assets/files/updated.json)

## Delegation & Linking (S3 only)
To support files stored in the cloud, Delta allows users to register files already uploaded to S3. This is useful primarily for large files where uploading directly through Delta using HTTP is inefficient and expensive.

There are two use cases: registering an already uploaded file by specifying its path, and asking Delta to generate a path in its standard format.

### Linking a new file using POST

This endpoint accepts a path and creates a new file resource based on an existing S3 file.

```
POST /v1/link/files/{org_label}/{project_label}?storage={storageId}&tag={tagName}
  {
    "path": "{path}",
    "mediaType": "{mediaType}",
    "metadata": {metadata}
  }
```

... where

- `{path}`: String - the relative path to the file from the root of S3.
- `{mediaType}`: String - Optional @link:[MIME](https://en.wikipedia.org/wiki/MIME){ open=new } specifying the file type. If omitted this will be inferred by S3.
- `{metadata}`: JSON Object - Optional, see @ref:[custom file metadata](#custom-file-metadata).
- `{storageId}` selects a specific storage backend where the file will be uploaded. This field is optional.
  When not specified, the default storage of the project is used.
- `{tagName}` an optional label given to the file on its first revision.

**Example**

Request
:   @@snip [link-post.sh](assets/files/link-post.sh)

Response
:   @@snip [link-result.json](assets/files/link-result.json)

### Linking a new file / update an existing file using PUT

This endpoint accepts a path and creates a new file resource or update an existing one based on an existing S3 file.

This alternative endpoint allows to specify the file `@id`.

```
PUT /v1/link/files/{org_label}/{project_label}/{fileId}?storage={storageId}&rev={previous_rev}&tag={tagName}
  {
    "path": "{path}",
    "mediaType": "{mediaType}",
    "metadata": {metadata}
  }
```

... where

- `{path}`: String - the relative path to the file from the root of S3.
- `{mediaType}`: String - Optional @link:[MIME](https://en.wikipedia.org/wiki/MIME){ open=new } specifying the file type. If omitted this will be inferred by S3.
- `{metadata}`: JSON Object - Optional, see @ref:[custom file metadata](#custom-file-metadata).
- `{storageId}` selects a specific storage backend where the file will be uploaded. This field is optional.
  When not specified, the default storage of the project is used.
- `{previous_rev}`: the last known revision number for the file (if a file for `{file_id}` already exists on that project).
- `{tagName}` an optional label given to the file on the new revision.

**Example**

Request
:   @@snip [link-put.sh](assets/files/link-put.sh)

Response
:   @@snip [link-result.json](assets/files/link-result.json)

### Delegating file uploads

Clients can use delegation for files that cannot be uploaded through Delta (e.g. large files which may benefit from multipart upload from S3).
Here Delta will provide bucket and path details for the upload. Users are then expected to upload the file using other methods, and call back to Delta to register this file with the same metadata that was initially validated. The three steps are outlined in detail below.

The following diagram illustrates the process of uploading a large file using the delegation method:
![delegation](assets/files/multipart-upload.png "Multipart upload process")

There is Delta @link:[configuration](https://github.com/senscience/nexus-delta/blob/master/delta/app/src/main/resources/app.conf){ open=new } to be done prior to using this feature, in particular setting up the necessary RSA key to support secure communication between Delta and AWS and signing payloads.

The configuration can be found under the following key `app.jws`. Please note that the allowed duration of the process is configured there with a default of 3h. Check the configuration key `app.jws.ttl` to change this value.

#### 1. Validate and generate path for file delegation

Delta accepts and validates the following payload.

##### Generate a delegation for a new file using POST
```
POST /v1/delegate/files/generate/{org_label}/{project_label}?storage={storageId}&tag={tagName}
  {
    "filename": "{filename}",
    "mediaType": "{mediaType}",
    "metadata": {metadata}
  }
```

... where

- `{filename}`: String - mandatory name given to the file within the generated path.
- `{mediaType}`: String - Optional @link:[MIME](https://en.wikipedia.org/wiki/MIME){ open=new } specifying the file type. If omitted this will be inferred by S3.
- `{metadata}`: JSON Object - Optional, see @ref:[custom file metadata](#custom-file-metadata).
- `{storageId}` selects a specific storage backend where the file will be uploaded. This field is optional.
  When not specified, the default storage of the project is used.
- `{tagName}`: String - an optional label given to the file on the new revision.

##### Generate a delegation for a new file / update an existing file using PUT

This endpoint accepts a path and creates a new file resource or update an existing one based on an existing S3 file.

This alternative endpoint allows to specify the file `@id`.

```
PUT /v1/delegate/files/generate/{org_label}/{project_label}/{fileId}?storage={storageId}&rev={previous_rev}&tag={tagName}
  {
    "filename": "{filename}",
    "mediaType": "{mediaType}",
    "metadata": {metadata}
  }
```

... where

- `{filename}`: String - mandatory name given to the file within the generated path.
- `{mediaType}`: String - Optional @link:[MIME](https://en.wikipedia.org/wiki/MIME){ open=new } specifying the file type. If omitted this will be inferred by S3.
- `{metadata}`: JSON Object - Optional, see @ref:[custom file metadata](#custom-file-metadata).
- `{storageId}` selects a specific storage backend where the file will be uploaded. This field is optional.
  When not specified, the default storage of the project is used.
- `{previous_rev}`: the last known revision number for the file (if a file for `{file_id}` already exists on that project).
- `{tagName}`: String - an optional label given to the file on the new revision.

##### Delegation payload

It then generates the following details for the file:

```json
 {
    "@type": "FileDelegationCreationRequest",
    "project": "{org_label}/{project_label}",
    "id": "<file resource identifier>",
    "rev": "<last known revision>",
    "targetLocation": {
      "storageId": "<storage identifier>",
      "bucket": "<s3 bucket>",
      "path": "<path from s3 root>"
    },
    "description": {
      "filename": "<filename>",
      "mediaType": "<user provided mediaType>",
      "metadata": {}
    }
 }
```

The user is expected to upload their file to `path` within `bucket`. The `id` is reserved for when the file resource is created. `mediaType` and `metadata` are what the user specified in the request.

This payload is then signed using the [flattened JWS serialization format](https://datatracker.ietf.org/doc/html/rfc7515#section-7.2.2) to ensure that Delta has validated and generated the correct data. This same payload will be passed when creating the file resource.

```json
 {
    "payload": "<base64 encoded payload contents>",
    "protected": "<integrity-protected header contents>",
    "signature": "<signature contents>"
 }
```

The `payload` field can be base64 decoded to access the generated file details. Note that `protected` contains an expiry field `exp` with the datetime at which this `signature` will expire (in epoch seconds). To view this can also be base64 decoded.

**Example**

Request
:   @@snip [delegate-validate-post.sh](assets/files/delegate-submit-post.sh)

Response
:   @@snip [delegate-validate-post.json](assets/files/delegate-submit-post.json)

#### 2. Upload file to S3

Using the bucket and path from the previous step, the file should be uploaded to S3 by whatever means are appropriate. The only restriction is that this must be finished before the expiry datetime of the signed payload.

Here are some tutorials in both @link:[Java with AWS SDK](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html#mpu-upload-low-level){ open=new } and
@link:[Python with Boto3](https://medium.com/analytics-vidhya/aws-s3-multipart-upload-download-using-boto3-python-sdk-2dedb0945f11){ open=new } that illustrate the process of doing a multipart upload to S3.

#### 3. Create/update delegated file resource

Once the file has been uploaded to S3 at the specified path, the file resource can be created in the knowledge graph. The payload can be passed back exactly as it was returned in the previous step.

```
POST /v1/delegate/files/submit
   {
      "payload": "<base64 encoded payload contents>",
      "protected": "<integrity-protected header contents>",
      "signature": "<signature contents>"
   }
```

Delta will verify that the `signature` matches the payload and that the expiry date is not passed. Then the file will be registered as a resource. The usual file resource response will be returned with all the standard metadata and file location details.

**Example**

Request
:   @@snip [delegate-create-post.sh](assets/files/delegate-create-post.sh)

Response
:   @@snip [delegate-create-post.json](assets/files/delegate-create-post.json)


## Tag

Links a file revision to a specific name.

Tagging a file is considered to be an update as well.

```
POST /v1/files/{org_label}/{project_label}/{file_id}/tags?rev={previous_rev}
  {
    "tag": "{name}",
    "rev": {rev}
  }
```

... where

- `{previous_rev}`: is the last known revision number for the file.
- `{name}`: String - label given to the file at specific revision.
- `{rev}`: Number - the revision to link the provided `{name}`.

**Example**

Request
:   @@snip [tag.sh](assets/files/tag.sh)

Payload
:   @@snip [tag.json](assets/tag.json)

Response
:   @@snip [tagged.json](assets/files/tagged.json)

## Remove tag

Removes a given tag.

Removing a tag is considered to be an update as well.

```
DELETE /v1/files/{org_label}/{project_label}/{file_id}/tags/{tag_name}?rev={previous_rev}
```
... where

- `{previous_rev}`: is the last known revision number for the resource.
- `{tag_name}`: String - label of the tag to remove.

**Example**

Request
:   @@snip [tag.sh](assets/files/delete-tag.sh)

Response
:   @@snip [tagged.json](assets/files/tagged.json)

## Deprecate

Locks the file, so no further operations can be performed.

Deprecating a file is considered to be an update as well. 

```
DELETE /v1/files/{org_label}/{project_label}/{file_id}?rev={previous_rev}
```

... where `{previous_rev}` is the last known revision number for the file.

**Example**

Request
:   @@snip [deprecate.sh](assets/files/deprecate.sh)

Response
:   @@snip [deprecated.json](assets/files/deprecated.json)

## Undeprecate

Unlocks a previously deprecated file. Further operations can then be performed. The file will again be found when listing/querying.

Undeprecating a file is considered to be an update as well.

```
PUT /v1/file/{org_label}/{project_label}/{file_id}/undeprecate?rev={previous_rev}
```

... where `{previous_rev}` is the last known revision number for the resource.

**Example**

Request
:   @@snip [undeprecate.sh](assets/files/undeprecate.sh)

Response
:   @@snip [undeprecated.json](assets/files/undeprecated.json)

## Fetch

When fetching a file, the response format can be chosen through HTTP content negotiation. 
In order to fetch the file metadata, the client can use any of the @ref:[following MIME types](content-negotiation.md#supported-mime-types).
However, in order to fetch the file content, the HTTP `Accept` header  `*/*` (or any MIME type that matches the file MediaType) should be provided.

```
GET /v1/files/{org_label}/{project_label}/{file_id}?rev={rev}&tag={tag}
```

where ...

- `{rev}`: Number - the targeted revision to be fetched. This field is optional and defaults to the latest revision.
- `{tag}`: String - the targeted tag to be fetched. This field is optional.

`{rev}` and `{tag}` fields cannot be simultaneously present.

**Example**

Request (binary)
:   @@snip [fetch.sh](assets/files/fetch.sh)

Request (metadata)
:   @@snip [fetch-metadata.sh](assets/files/fetch-metadata.sh)

Response  (metadata)
:   @@snip [fetched-metadata.json](assets/files/fetched-metadata.json)

If the @ref:[redirect to Fusion feature](../../running-nexus/configuration/index.md#fusion-configuration) is enabled and
if the `Accept` header is set to `text/html`, a redirection to the fusion representation of the resource will be returned.

## Fetch tags

Retrieves all the tags available for the `{file_id}`.

```
GET /v1/files/{org_label}/{project_label}/{file_id}/tags?rev={rev}&tag={tag}
```

where ...

- `{rev}`: Number - the targeted revision of the tags to be fetched. This field is optional and defaults to the latest revision.
- `{tag}`: String - the targeted tag of the tags to be fetched. This field is optional.

`{rev}` and `{tag}` fields cannot be simultaneously present.

**Example**

Request
:   @@snip [fetch-tags.sh](assets/files/fetch-tags.sh)

Response
:   @@snip [fetched-tags.json](assets/tags.json)

## List

There are three available endpoints to list files in different scopes.

### Within a project

```
GET /v1/files/{org_label}/{project_label}?from={from}
                                         &size={size}
                                         &deprecated={deprecated}
                                         &rev={rev}
                                         &type={type}
                                         &createdBy={createdBy}
                                         &updatedBy={updatedBy}
                                         &q={search}
                                         &sort={sort}
                                         &aggregations={aggregations}
```

### Within an organization

This operation returns only files from projects defined in the organisation `{org_label}` and where the caller has the `resources/read` permission.

```
GET /v1/files/{org_label}?from={from}
                         &size={size}
                         &deprecated={deprecated}
                         &rev={rev}
                         &type={type}
                         &createdBy={createdBy}
                         &updatedBy={updatedBy}
                         &q={search}
                         &sort={sort}
                         &aggregations={aggregations}
```

### Within all projects

This operation returns only files from projects where the caller has the `resources/read` permission.

```
GET /v1/files?from={from}
             &size={size}
             &deprecated={deprecated}
             &rev={rev}
             &type={type}
             &createdBy={createdBy}
             &updatedBy={updatedBy}
             &q={search}
             &sort={sort}
             &aggregations={aggregations}
```

### Parameter description

- `{from}`: Number - is the parameter that describes the offset for the current query; defaults to `0`
- `{size}`: Number - is the parameter that limits the number of results; defaults to `20`
- `{deprecated}`: Boolean - can be used to filter the resulting files based on their deprecation status
- `{rev}`: Number - can be used to filter the resulting files based on their revision value
- `{type}`: Iri - can be used to filter the resulting files based on their `@type` value. This parameter can appear
  multiple times, filtering further the `@type` value.
- `{createdBy}`: Iri - can be used to filter the resulting files based on their creator
- `{updatedBy}`: Iri - can be used to filter the resulting files based on the person which performed the last update
- `{search}`: String - can be provided to select only the files in the collection that have attribute values matching
  (containing) the provided string
- `{sort}`: String - can be used to sort files based on a payloads' field. This parameter can appear multiple times
  to enable sorting by multiple fields. The default is done by `_createdBy` and `@id`.
- `{aggregations}`: Boolean - if `true` then the response will only contain aggregations of the `@type` and `_project` fields; defaults to `false`. See @ref:[Aggregations](resources-api.md#aggregations).

**Example**

Request
:   @@snip [list.sh](assets/files/list.sh)

Response
:   @@snip [listed.json](assets/files/listed.json)

## Server Sent Events

From Delta 1.5, it is possible to fetch SSEs for all files or just files
in the scope of an organization or a project.

```
GET /v1/files/events                              # for all file events in the application
GET /v1/files/{org_label}/events                  # for file events in the given organization
GET /v1/files/{org_label}/{project_label}/events  # for file events in the given project
```

The caller must have respectively the `events/read` permission on `/`, `{org_label}` and `{org_label}/{project_label}`.

- `{org_label}`: String - the selected organization for which the events are going to be filtered
- `{project_label}`: String - the selected project for which the events are going to be filtered
- `Last-Event-Id`: String - optional HTTP Header that identifies the last consumed resource event. It can be used for
  cases when a client does not want to retrieve the whole event stream, but to start after a specific event.

@@@ note { .warning }

The event type for files SSEs have been changed so that it is easier to distinguish them from other types of resources.

@@@

**Example**

Request
:   @@snip [sse.sh](assets/files/sse.sh)

Response
:   @@snip [sse.json](assets/files/sse.json)
