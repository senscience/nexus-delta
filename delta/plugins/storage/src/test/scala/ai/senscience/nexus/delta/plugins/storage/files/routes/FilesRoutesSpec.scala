package ai.senscience.nexus.delta.plugins.storage.files.routes

import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.`application/ld+json`
import ai.senscience.nexus.delta.plugins.storage.files.mocks.FileOperationsMock
import ai.senscience.nexus.delta.plugins.storage.files.model.Digest.ComputedDigest
import ai.senscience.nexus.delta.plugins.storage.files.model.{FileAttributes, FileId}
import ai.senscience.nexus.delta.plugins.storage.files.{contexts as fileContexts, permissions, FileFixtures, Files, FormDataExtractor, MediaTypeDetector, MediaTypeDetectorConfig}
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageType
import ai.senscience.nexus.delta.plugins.storage.storages.operations.{FileOperations, LinkFileAction}
import ai.senscience.nexus.delta.plugins.storage.storages.{contexts as storageContexts, permissions as storagesPermissions, FetchStorage, StorageFixtures, Storages, StoragesConfig}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.identities.model.{Caller, ServiceAccount}
import ai.senscience.nexus.delta.sdk.identities.{Identities, IdentitiesDummy}
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceAccess}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.events
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sdk.{IndexingAction, NexusHeaders}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef}
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.errors.files.FileErrors.{fileAlreadyExistsError, fileIsNotDeprecatedError}
import ai.senscience.nexus.testkit.scalatest.FileMatchers.*
import ai.senscience.nexus.testkit.scalatest.ce.CatsIOValues
import akka.http.scaladsl.model.*
import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.MediaRanges.*
import akka.http.scaladsl.model.MediaTypes.{`multipart/form-data`, `text/html`}
import akka.http.scaladsl.model.headers.*
import akka.http.scaladsl.server.Route
import cats.effect.IO
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Json, JsonObject}
import org.scalatest.*

import java.util.UUID

class FilesRoutesSpec
    extends BaseRouteSpec
    with CancelAfterFailure
    with StorageFixtures
    with FileFixtures
    with CatsIOValues {

  // TODO: sort out how we handle this in tests
  implicit override def rcr: RemoteContextResolution =
    RemoteContextResolution.fixedIO(
      storageContexts.storages         -> ContextValue.fromFile("contexts/storages.json"),
      storageContexts.storagesMetadata -> ContextValue.fromFile("contexts/storages-metadata.json"),
      fileContexts.files               -> ContextValue.fromFile("contexts/files.json"),
      Vocabulary.contexts.metadata     -> ContextValue.fromFile("contexts/metadata.json"),
      Vocabulary.contexts.error        -> ContextValue.fromFile("contexts/error.json"),
      Vocabulary.contexts.tags         -> ContextValue.fromFile("contexts/tags.json"),
      Vocabulary.contexts.search       -> ContextValue.fromFile("contexts/search.json")
    )

  private val reader   = User("reader", realm)
  private val writer   = User("writer", realm)
  private val s3writer = User("s3writer", realm)

  private val identities = IdentitiesDummy.fromUsers(reader, writer, s3writer)

  private val fetchContext = FetchContextDummy(Map(project.ref -> project.context))

  private val s3Read    = Permission.unsafe("s3/read")
  private val s3Write   = Permission.unsafe("s3/write")
  private val diskRead  = Permission.unsafe("disk/read")
  private val diskWrite = Permission.unsafe("disk/write")

  override val allowedPerms: Seq[Permission] =
    Seq(
      permissions.read,
      permissions.write,
      storagesPermissions.write,
      events.read,
      s3Read,
      s3Write,
      diskRead,
      diskWrite
    )

  private val stCfg = config.copy(disk = config.disk.copy(defaultMaxFileSize = 1000, allowedVolumes = Set(path)))

  private val aclCheck = AclSimpleCheck().accepted

  lazy val storages: Storages         = Storages(
    fetchContext,
    ResolverContextResolution(rcr),
    IO.pure(allowedPerms.toSet),
    _ => IO.unit,
    xas,
    StoragesConfig(eventLogConfig, pagination, stCfg),
    ServiceAccount(User("nexus-sa", Label.unsafe("sa"))),
    clock
  ).accepted
  lazy val fileOps: FileOperations    = FileOperationsMock.disabled
  lazy val fetchStorage: FetchStorage = FetchStorage(storages, aclCheck)

  private val mediaTypeDetector = new MediaTypeDetector(MediaTypeDetectorConfig.Empty)
  private val dataExtractor     = FormDataExtractor(mediaTypeDetector)(system)
  private val linkAction        = LinkFileAction.alwaysFails
  lazy val files: Files         =
    Files(fetchContext, fetchStorage, dataExtractor, xas, eventLogConfig, fileOps, linkAction, clock)(uuidF)
  private val groupDirectives   = DeltaSchemeDirectives(fetchContext)
  private lazy val routes       = routesWithIdentities(identities)

  private def routesWithIdentities(identities: Identities) =
    Route.seal(FilesRoutes(identities, aclCheck, files, groupDirectives, IndexingAction.noop))

  private val diskIdRev = ResourceRef.Revision(dId, 1)
  private val s3IdRev   = ResourceRef.Revision(s3Id, 2)
  private val tag       = "mytag"

  private val varyHeader = RawHeader("Vary", "Accept,Accept-Encoding")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val writePermissions = Set(storagesPermissions.write, diskWrite, permissions.write)
    val readPermissions  = Set(diskRead, s3Read, permissions.read)
    aclCheck.append(AclAddress.Root, writer -> writePermissions, writer -> readPermissions).accepted
    aclCheck.append(AclAddress.Root, writer -> writePermissions).accepted
    aclCheck.append(AclAddress.Root, reader -> readPermissions).accepted
    aclCheck.append(AclAddress.Root, s3writer -> Set(s3Write), s3writer -> Set(s3Write)).accepted

    val defaults = json"""{"maxFileSize": 1000, "volume": "$path"}"""
    val s3Perms  = json"""{"readPermission": "$s3Read", "writePermission": "$s3Write"}"""
    storages.create(s3Id, projectRef, diskFieldsJson deepMerge defaults deepMerge s3Perms)(Caller(writer)).accepted
    storages.create(dId, projectRef, diskFieldsJson deepMerge defaults)(Caller(s3writer)).void.accepted
  }

  def postJson(path: String, json: Json): HttpRequest = {
    Post(path, json.toEntity).withHeaders(`Content-Type`(`application/json`))
  }

  def postFile(path: String, entity: RequestEntity): HttpRequest = {
    Post(path, entity).withHeaders(`Content-Type`(`multipart/form-data`))
  }

  def postFileWithMetadata(path: String, entity: RequestEntity, metadata: Json): HttpRequest = {
    val headers =
      `Content-Type`(`multipart/form-data`) :: RawHeader(NexusHeaders.fileMetadata, metadata.noSpaces) :: Nil
    Post(path, entity).withHeaders(headers)
  }

  def putJson(path: String, json: Json): HttpRequest = {
    Put(path, json.toEntity).withHeaders(`Content-Type`(`application/json`))
  }

  def putFile(path: String, entity: RequestEntity): HttpRequest = {
    Put(path, entity).withHeaders(`Content-Type`(`multipart/form-data`))
  }

  def putFileWithMetadata(path: String, entity: RequestEntity, metadata: Json): HttpRequest = {
    val headers =
      `Content-Type`(`multipart/form-data`) :: RawHeader(NexusHeaders.fileMetadata, metadata.noSpaces) :: Nil
    Put(path, entity).withHeaders(headers)
  }

  "File routes" should {

    "fail to create a file without disk/write permission" in {
      postFile("/v1/files/org/proj", entity()) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "create a file" in {
      postFile("/v1/files/org/proj", entity()) ~> as(writer) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        val attr = attributes(path)
        response.asJson shouldEqual fileMetadata(projectRef, generatedId, attr, diskIdRev)
      }
    }

    "create a file with metadata" in {
      withUUIDF(UUID.randomUUID()) {
        val metadata = genCustomMetadata()
        val kw       = metadata.keywords.get.map { case (k, v) => k.toString -> v }
        val file     = entity(genString())

        postFileWithMetadata("/v1/files/org/proj", file, metadata.asJson) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          response.asJson should have(description(metadata.description.get))
          response.asJson should have(name(metadata.name.get))
          response.asJson should have(keywords(kw))
        }
      }
    }

    "fail to create a file with invalid metadata" in {
      val invalidKey      = Label.unsafe("!@#$%^&")
      val invalidMetadata = genCustomMetadata().copy(keywords = Some(Map(invalidKey -> "value")))
      val id              = genString()

      putFileWithMetadata(s"/v1/files/org/proj/$id", entity(), invalidMetadata.asJson) ~> as(
        writer
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        response.asJson shouldEqual
          json"""
              {
                "@context" : "https://bluebrain.github.io/nexus/contexts/error.json",
                "@type" : "MalformedHeaderRejection",
                "reason" : "The value of HTTP header 'x-nxs-file-metadata' was malformed.",
                "details" : "DecodingFailure at .keywords.$invalidKey: Couldn't decode key."
              }
                """

        Get(s"/v1/files/org/proj/$id") ~> Accept(`*/*`) ~> as(reader) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
        }
      }
    }

    "create and tag a file" in {
      withUUIDF(uuid2) {
        postFile("/v1/files/org/proj?tag=mytag", entity()) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          val attr      = attributes(path, id = uuid2)
          val expected  = fileMetadata(projectRef, generatedId2, attr, diskIdRev)
          val userTag   = UserTag.unsafe(tag)
          val fileByTag = files.fetch(FileId(generatedId2, userTag, projectRef)).accepted
          response.asJson shouldEqual expected
          fileByTag.value.tags.tags should contain(userTag)
        }
      }
    }

    "fail to create a file without s3/write permission" in {
      putFile("/v1/files/org/proj/file1?storage=s3-storage", entity()) ~> as(writer) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "create a file on s3 with an authenticated user and provided id" in {
      val id = genString()
      putFile(s"/v1/files/org/proj/$id?storage=s3-storage", entity(id)) ~> as(s3writer) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        val attr = attributes(path, id)
        response.asJson shouldEqual
          fileMetadata(projectRef, nxv + id, attr, s3IdRev, createdBy = s3writer, updatedBy = s3writer)
      }
    }

    "create and tag a file on s3 with an authenticated user and provided id" in {
      withUUIDF(uuid2) {
        putFile(
          "/v1/files/org/proj/fileTagged?storage=s3-storage&tag=mytag",
          entity("fileTagged.txt")
        ) ~> as(s3writer) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          val attr      = attributes(path, "fileTagged.txt", id = uuid2)
          val expected  = fileMetadata(projectRef, fileTagged, attr, s3IdRev, createdBy = s3writer, updatedBy = s3writer)
          val userTag   = UserTag.unsafe(tag)
          val fileByTag = files.fetch(FileId(generatedId2, userTag, projectRef)).accepted
          response.asJson shouldEqual expected
          fileByTag.value.tags.tags should contain(userTag)
        }
      }
    }

    "reject the creation of a file which already exists" in {
      givenAFile { id =>
        putFile(s"/v1/files/org/proj/$id", entity()) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.Conflict
          response.asJson shouldEqual fileAlreadyExistsError(nxvBase(id))
        }
      }
    }

    "reject the creation of a file that is too large" in {
      putFile(
        "/v1/files/org/proj/file-too-large",
        randomEntity(filename = "large-file.txt", 1100)
      ) ~> as(writer) ~> routes ~> check {
        status shouldEqual StatusCodes.PayloadTooLarge
        response.asJson shouldEqual jsonContentOf("files/errors/file-too-large.json")
      }
    }

    "reject the creation of a file to a storage that does not exist" in {
      putFile("/v1/files/org/proj/file2?storage=not-exist", entity()) ~> as(writer) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        response.asJson shouldEqual
          jsonContentOf("storages/errors/not-found.json", "id" -> (nxv + "not-exist"), "proj" -> projectRef)
      }
    }

    "fail to update a file without disk/write permission" in {
      givenAFile { id =>
        putJson(s"/v1/files/org/proj/$id?rev=1", s3FieldsJson) ~> routes ~> check {
          response.shouldBeForbidden
        }
      }
    }

    "update a file" in {
      givenAFile { id =>
        val endpoints = List(
          s"/v1/files/org/proj/$id",
          s"/v1/files/org/proj/${encodeId(id)}"
        )
        forAll(endpoints.zipWithIndex) { case (endpoint, idx) =>
          val filename = s"file-idx-$idx.txt"
          putFile(s"$endpoint?rev=${idx + 1}", entity(filename)) ~> as(writer) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            val attr = attributes(path, filename)
            response.asJson shouldEqual
              fileMetadata(projectRef, nxv + id, attr, diskIdRev, rev = idx + 2)
          }
        }
      }
    }

    "update a file with metadata" in {
      givenAFile { id =>
        val metadata = genCustomMetadata()
        val kw       = metadata.keywords.get.map { case (k, v) => k.toString -> v }

        putFileWithMetadata(
          s"/v1/files/org/proj/$id?rev=1",
          entity(genString()),
          metadata.asJson
        ) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          response.asJson should have(description(metadata.description.get))
          response.asJson should have(name(metadata.name.get))
          response.asJson should have(keywords(kw))
        }
      }
    }

    "fail to update a file with invalid custom metadata" in {
      givenAFile { id =>
        val invalidKey      = Label.unsafe("!@#$%^&")
        val invalidMetadata = genCustomMetadata().copy(keywords = Some(Map(invalidKey -> "value")))

        putFileWithMetadata(
          s"/v1/files/org/proj/$id?rev=1",
          entity(genString()),
          invalidMetadata.asJson
        ) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual
            json"""
              {
                "@context" : "https://bluebrain.github.io/nexus/contexts/error.json",
                "@type" : "MalformedHeaderRejection",
                "reason" : "The value of HTTP header 'x-nxs-file-metadata' was malformed.",
                "details" : "DecodingFailure at .keywords.$invalidKey: Couldn't decode key."
              }
                """
        }
      }
    }

    "fail to update custom metadata without permission" in {
      givenAFile { id =>
        val metadata = genCustomMetadata()
        val headers  = RawHeader(NexusHeaders.fileMetadata, metadata.asJson.noSpaces)
        Put(s"/v1/files/org/proj/$id?rev=1").withHeaders(headers) ~> routes ~> check {
          response.shouldBeForbidden
        }
      }
    }

    "update only custom metadata with no entity provided" in {
      givenAFile { id =>
        val metadata = genCustomMetadata()
        val kw       = metadata.keywords.get.map { case (k, v) => k.toString -> v }
        val headers  = RawHeader(NexusHeaders.fileMetadata, metadata.asJson.noSpaces)
        Put(s"/v1/files/org/proj/$id?rev=1").withHeaders(headers) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          response.asJson should have(description(metadata.description.get))
          response.asJson should have(name(metadata.name.get))
          response.asJson should have(keywords(kw))
        }
      }
    }

    "allow tagging when updating custom metadata" in {
      givenAFile { id =>
        val metadata = genCustomMetadata()
        val userTag  = UserTag.unsafe("mytag")

        val headers = RawHeader(NexusHeaders.fileMetadata, metadata.asJson.noSpaces)
        Put(s"/v1/files/org/proj/$id?rev=1&tag=${userTag.value}").withHeaders(headers) ~> as(
          writer
        ) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          Get(s"/v1/files/org/proj/$id?tag=${userTag.value}") ~> Accept(
            MediaTypes.`application/json`
          ) ~> as(reader) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
          }
        }
      }
    }

    "return an error when attempting to update custom metadata without providing it" in {
      givenAFile { id =>
        Put(s"/v1/files/org/proj/$id?rev=1") ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual
            json"""
                {
                  "@context" : "https://bluebrain.github.io/nexus/contexts/error.json",
                  "@type" : "EmptyCustomMetadata",
                  "reason" : "No metadata was provided"
                }
                  """
        }
      }
    }

    "return an error when attempting to update with invalid custom metadata" in {
      givenAFile { id =>
        val invalidKey      = Label.unsafe("!@#$%^&")
        val invalidMetadata = genCustomMetadata().copy(keywords = Some(Map(invalidKey -> "value")))

        val headers = RawHeader(NexusHeaders.fileMetadata, invalidMetadata.asJson.noSpaces)
        Put(s"/v1/files/org/proj/$id?rev=1").withHeaders(headers) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual
            json"""
                  {
                    "@context" : "https://bluebrain.github.io/nexus/contexts/error.json",
                    "@type" : "MalformedHeaderRejection",
                    "reason" : "The value of HTTP header 'x-nxs-file-metadata' was malformed.",
                    "details" : "DecodingFailure at .keywords.$invalidKey: Couldn't decode key."
                  }
                    """
        }
      }
    }

    "update and tag a file in one request" in {
      givenAFile { id =>
        putFile(s"/v1/files/org/proj/$id?rev=1&tag=mytag", entity(s"$id.txt")) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
        }
        Get(s"/v1/files/org/proj/$id?tag=mytag") ~> Accept(`*/*`) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
    }

    "reject the update of a non-existent file" in {
      val nonExistentFile = genString()
      putFile(s"/v1/files/org/proj/$nonExistentFile?rev=1", entity("other.txt")) ~> as(writer) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        response.asJson shouldEqual
          jsonContentOf("files/errors/not-found.json", "id" -> (nxv + nonExistentFile), "proj" -> "org/proj")
      }
    }

    "reject the update of a non-existent file storage" in {
      givenAFile { id =>
        putFile(s"/v1/files/org/proj/$id?rev=1&storage=not-exist", entity("other.txt")) ~> as(
          writer
        ) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
          response.asJson shouldEqual
            jsonContentOf("storages/errors/not-found.json", "id" -> (nxv + "not-exist"), "proj" -> projectRef)
        }
      }
    }

    "reject the update of a file at a non-existent revision" in {
      givenAFile { id =>
        putFile(s"/v1/files/org/proj/$id?rev=10", entity("other.txt")) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.Conflict
          response.asJson shouldEqual
            jsonContentOf("files/errors/incorrect-rev.json", "provided" -> 10, "expected" -> 1)
        }
      }
    }

    "fail to deprecate a file without files/write permission" in {
      givenAFile { id =>
        Delete(s"/v1/files/org/proj/$id?rev=1") ~> routes ~> check {
          response.shouldBeForbidden
        }
      }
    }

    "deprecate a file" in {
      givenAFile { id =>
        Delete(s"/v1/files/org/proj/$id?rev=1") ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          val attr = attributes(path, id)
          response.asJson shouldEqual fileMetadata(projectRef, nxv + id, attr, diskIdRev, rev = 2, deprecated = true)
        }
      }
    }

    "reject the deprecation of a file without rev" in {
      givenAFile { id =>
        Delete(s"/v1/files/org/proj/$id") ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual jsonContentOf("errors/missing-query-param.json", "field" -> "rev")
        }
      }
    }

    "reject the deprecation of an already deprecated file" in {
      givenADeprecatedFile { id =>
        Delete(s"/v1/files/org/proj/$id?rev=2") ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual jsonContentOf("files/errors/file-deprecated.json", "id" -> (nxv + id))
        }
      }
    }

    "fail to undeprecate a file without files/write permission" in {
      givenADeprecatedFile { id =>
        Put(s"/v1/files/org/proj/$id/undeprecate?rev=2") ~> as(reader) ~> routes ~> check {
          response.shouldBeForbidden
        }
      }
    }

    "undeprecate a file" in {
      givenADeprecatedFile { id =>
        Put(s"/v1/files/org/proj/$id/undeprecate?rev=2") ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          response.asJson shouldEqual
            fileMetadata(projectRef, nxv + id, attributes(path, id), diskIdRev, rev = 3, deprecated = false)

          Get(s"/v1/files/org/proj/$id") ~> Accept(`*/*`) ~> as(reader) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
          }
        }
      }
    }

    "reject the undeprecation of a file without rev" in {
      givenADeprecatedFile { id =>
        Put(s"/v1/files/org/proj/$id/undeprecate") ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual jsonContentOf("errors/missing-query-param.json", "field" -> "rev")
        }
      }
    }

    "reject the undeprecation of a file that is not deprecated" in {
      givenAFile { id =>
        Put(s"/v1/files/org/proj/$id/undeprecate?rev=1") ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual fileIsNotDeprecatedError(nxvBase(id))
        }
      }
    }

    "tag a file" in {
      givenAFile { id =>
        val payload = json"""{"tag": "mytag", "rev": 1}"""
        postJson(s"/v1/files/org/proj/$id/tags?rev=1", payload) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          val attr = attributes(path, id)
          response.asJson shouldEqual fileMetadata(projectRef, nxv + id, attr, diskIdRev, rev = 2)
        }
      }
    }

    "fail to fetch a file without s3/read permission" in {
      givenATaggedFile(tag) { id =>
        forAll(List("", "?rev=1", s"?tags=$tag")) { suffix =>
          Get(s"/v1/files/org/proj/$id$suffix") ~> Accept(`*/*`) ~> routes ~> check {
            response.shouldBeForbidden
            response.headers should not contain varyHeader
          }
        }
      }
    }

    "fail to fetch a file when the accept header does not match file media type" in {
      givenATaggedFile(tag) { id =>
        forAll(List("", "?rev=1", s"?tags=$tag")) { suffix =>
          Get(s"/v1/files/org/proj/$id$suffix") ~> Accept(`video/*`) ~> as(reader) ~> routes ~> check {
            response.status shouldEqual StatusCodes.NotAcceptable
            response.asJson shouldEqual jsonContentOf("errors/content-type.json", "expected" -> "text/plain")
            response.headers should not contain varyHeader
          }
        }
      }
    }

    "fetch a file" in {
      givenAFile { id =>
        forAll(List(Accept(`*/*`), Accept(`text/*`))) { accept =>
          forAll(
            List(s"/v1/files/org/proj/$id", s"/v1/resources/org/proj/_/$id", s"/v1/resources/org/proj/file/$id")
          ) { endpoint =>
            Get(endpoint) ~> accept ~> as(reader) ~> routes ~> check {
              status shouldEqual StatusCodes.OK
              contentType.value shouldEqual "text/plain"
              header("Content-Disposition").value.value() shouldEqual
                s"""attachment; filename="=?UTF-8?B?${base64encode(id)}?=""""
              response.asString shouldEqual content
              response.expectConditionalCacheHeaders
              response.headers should contain(varyHeader)
            }
          }
        }
      }
    }

    "fetch a file by rev and tag" in {
      givenATaggedFile(tag) { id =>
        val endpoints = List(
          s"/v1/files/org/proj/$id",
          s"/v1/resources/org/proj/_/$id",
          s"/v1/resources/org/proj/file/$id",
          s"/v1/files/org/proj/${encodeId(id)}",
          s"/v1/resources/org/proj/_/${encodeId(id)}",
          s"/v1/resources/org/proj/file/${encodeId(id)}"
        )
        forAll(endpoints) { endpoint =>
          forAll(List("rev=1", s"tag=$tag")) { param =>
            Get(s"$endpoint?$param") ~> Accept(`*/*`) ~> as(reader) ~> routes ~> check {
              status shouldEqual StatusCodes.OK
              contentType.value shouldEqual "text/plain"
              header("Content-Disposition").value.value() shouldEqual
                s"""attachment; filename="=?UTF-8?B?${base64encode(id)}?=""""
              response.asString shouldEqual content
              response.headers should contain(varyHeader)
            }
          }
        }
      }
    }

    "fail to fetch a file metadata without resources/read permission" in {
      givenATaggedFile(tag) { id =>
        val endpoints =
          List(s"/v1/files/org/proj/$id", s"/v1/files/org/proj/$id/tags", s"/v1/resources/org/proj/_/$id/tags")
        forAll(endpoints) { endpoint =>
          forAll(List("", "?rev=1", s"?tags=$tag")) { suffix =>
            Get(s"$endpoint$suffix") ~> Accept(`application/ld+json`) ~> routes ~> check {
              response.shouldBeForbidden
              response.headers should not contain varyHeader
            }
          }
        }
      }
    }

    "fetch a file metadata" in {
      givenAFile { id =>
        Get(s"/v1/files/org/proj/$id") ~> Accept(`application/ld+json`) ~> as(reader) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          val attr = attributes(path, id)
          response.asJson shouldEqual fileMetadata(projectRef, nxv + id, attr, diskIdRev)
          response.expectConditionalCacheHeaders
          response.headers should contain(varyHeader)
        }
      }
    }

    "fetch a file metadata by rev and tag" in {
      givenATaggedFile(tag) { id =>
        val attr      = attributes(path, id)
        val endpoints = List(
          s"/v1/files/org/proj/$id",
          s"/v1/resources/org/proj/_/$id",
          s"/v1/files/org/proj/${encodeId(id)}",
          s"/v1/resources/org/proj/_/${encodeId(id)}"
        )
        forAll(endpoints) { endpoint =>
          forAll(List("rev=1", s"tag=$tag")) { param =>
            Get(s"$endpoint?$param") ~> Accept(`application/ld+json`) ~> as(reader) ~> routes ~> check {
              status shouldEqual StatusCodes.OK
              response.asJson shouldEqual
                fileMetadata(projectRef, nxv + id, attr, diskIdRev)
              response.expectConditionalCacheHeaders
              response.headers should contain(varyHeader)
            }
          }
        }
      }
    }

    "fetch the file tags" in {
      givenAFile { id =>
        val taggingPayload = json"""{"tag": "$tag", "rev": 1}"""
        Post(s"/v1/files/org/proj/$id/tags?rev=1", taggingPayload.toEntity) ~> as(writer) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
        }
        Get(s"/v1/resources/org/proj/_/$id/tags?rev=1") ~> as(reader) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          response.asJson shouldEqual json"""{"tags": []}""".addContext(contexts.tags)
        }
        Get(s"/v1/files/org/proj/$id/tags") ~> as(reader) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          response.asJson shouldEqual json"""{"tags": [{"rev": 1, "tag": "$tag"}]}""".addContext(contexts.tags)
        }
      }
    }

    "return not found if tag not found" in {
      givenATaggedFile("mytag") { id =>
        Get(s"/v1/files/org/proj/$id?tag=myother") ~> Accept(`application/ld+json`) ~> as(reader) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
          response.asJson shouldEqual jsonContentOf("errors/tag-not-found.json", "tag" -> "myother")
        }
      }
    }

    "reject if provided rev and tag simultaneously" in {
      givenATaggedFile(tag) { id =>
        Get(s"/v1/files/org/proj/$id?tag=$tag&rev=1") ~> Accept(`application/ld+json`) ~> as(
          reader
        ) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual jsonContentOf("errors/tag-and-rev-error.json")
        }
      }
    }

    def deleteTag(id: String, tag: String, rev: Int) =
      Delete(s"/v1/files/org/proj/$id/tags/$tag?rev=$rev") ~> as(writer) ~> routes ~> check {
        val attr = attributes(path, s"$id")
        status shouldEqual StatusCodes.OK
        response.asJson shouldEqual fileMetadata(projectRef, nxv + id, attr, diskIdRev, rev = rev + 1)
      }

    "delete a tag on file" in {
      givenATaggedFile(tag) { id =>
        deleteTag(id, tag, 1)
      }
    }

    "not return the deleted tag" in {
      givenATaggedFile(tag) { id =>
        deleteTag(id, tag, 1)
        Get(s"/v1/files/org/proj/$id/tags") ~> as(reader) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          response.asJson shouldEqual json"""{"tags": []}""".addContext(contexts.tags)
        }
      }
    }

    "fail to fetch file by the deleted tag" in {
      givenATaggedFile(tag) { id =>
        deleteTag(id, tag, 1)
        Get(s"/v1/files/org/proj/$id?tag=$tag") ~> Accept(`application/ld+json`) ~> as(reader) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
          response.asJson shouldEqual jsonContentOf("errors/tag-not-found.json", "tag" -> "mytag")
        }
      }
    }

    "redirect to fusion for the latest version if the Accept header is set to text/html" in {
      givenAFile { id =>
        Get(s"/v1/files/org/project/$id") ~> Accept(`text/html`) ~> routes ~> check {
          response.status shouldEqual StatusCodes.SeeOther
          response.header[Location].value.uri shouldEqual Uri(
            s"https://bbp.epfl.ch/nexus/web/org/project/resources/$id"
          )
        }
      }
    }
  }

  def givenAFile(test: String => Assertion): Assertion = givenAFileInProject("org/proj")(test)

  def givenAFileInProject(projRef: String)(test: String => Assertion): Assertion = {
    val id = genString()
    Put(s"/v1/files/$projRef/$id", entity(s"$id")) ~> as(writer) ~> routes ~> check {
      status shouldEqual StatusCodes.Created
    }
    test(id)
  }

  def givenATaggedFile(tag: String)(test: String => Assertion): Assertion = {
    val id = genString()
    Put(s"/v1/files/org/proj/$id?tag=$tag", entity(s"$id")) ~> as(writer) ~> routes ~> check {
      status shouldEqual StatusCodes.Created
    }
    test(id)
  }

  def givenADeprecatedFile(test: String => Assertion): Assertion =
    givenAFile { id =>
      Delete(s"/v1/files/org/proj/$id?rev=1") ~> as(writer) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      test(id)
    }

  def fileMetadata(
      project: ProjectRef,
      id: Iri,
      attributes: FileAttributes,
      storage: ResourceRef.Revision,
      storageType: StorageType = StorageType.DiskStorage,
      rev: Int = 1,
      deprecated: Boolean = false,
      createdBy: Subject = writer,
      updatedBy: Subject = writer
  )(implicit baseUri: BaseUri): Json =
    FilesRoutesSpec
      .fileMetadata(project, id, attributes, storage, storageType, rev, deprecated, createdBy, updatedBy)

  private def nxvBase(id: String): String = (nxv + id).toString

}

object FilesRoutesSpec extends CirceLiteral {
  def fileMetadata(
      project: ProjectRef,
      id: Iri,
      attributes: FileAttributes,
      storage: ResourceRef.Revision,
      storageType: StorageType = StorageType.DiskStorage,
      rev: Int = 1,
      deprecated: Boolean = false,
      createdBy: Subject,
      updatedBy: Subject
  )(implicit baseUri: BaseUri): Json = {
    val self               = ResourceAccess("files", project, id).uri
    val keywordsJson: Json = attributes.keywords.isEmpty match {
      case false =>
        Json.obj(
          "_keywords" -> JsonObject
            .fromIterable(
              attributes.keywords.map { case (k, v) =>
                k.value -> v.asJson
              }
            )
            .toJson
        )
      case true  => Json.obj()
    }
    val descriptionJson    = attributes.description.map(desc => Json.obj("description" := desc))
    val nameJson           = attributes.name.map(name => Json.obj("name" := name))

    val mainJson = json"""
      {
        "@context" : [
          "https://bluebrain.github.io/nexus/contexts/files.json",
          "https://bluebrain.github.io/nexus/contexts/metadata.json"
        ],
        "@id" : "$id",
        "@type" : "File",
        "_constrainedBy" : "https://bluebrain.github.io/nexus/schemas/files.json",
        "_createdAt" : "1970-01-01T00:00:00Z",
        "_createdBy" : "${createdBy.asIri}",
        "_storage" : {
          "@id": "${storage.iri}",
          "@type": "$storageType",
          "_rev": ${storage.rev}
        },
        "_bytes": ${attributes.bytes},
        "_digest": {
          "_value": "${attributes.digest.asInstanceOf[ComputedDigest].value}",
          "_algorithm": "${attributes.digest.asInstanceOf[ComputedDigest].algorithm}"
        },
        "_filename": "${attributes.filename}",
        "_origin": "${attributes.origin}",
        "_mediaType": "${attributes.mediaType.fold("")(_.tree)}",
        "_deprecated" : $deprecated,
        "_project" : "$project",
        "_rev" : $rev,
        "_self" : "$self",
        "_updatedAt" : "1970-01-01T00:00:00Z",
        "_updatedBy" : "${updatedBy.asIri}",
        "_uuid": "${attributes.uuid}"
      }
    """

    (List(mainJson, keywordsJson) ++ nameJson ++ descriptionJson)
      .reduce(_.deepMerge(_))
  }
}
