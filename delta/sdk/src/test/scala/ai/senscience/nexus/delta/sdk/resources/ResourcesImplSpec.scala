package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv, schema, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.sdk.generators.{ProjectGen, ResourceGen}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.*
import ai.senscience.nexus.delta.sdk.model.{IdSegment, IdSegmentRef}
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectIsDeprecated, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.resolvers.model.ResourceResolutionReport
import ai.senscience.nexus.delta.sdk.resources.model.Resource
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.{ConfigFixtures, DataResource}
import ai.senscience.nexus.delta.sourcing.ScopedEventLog
import ai.senscience.nexus.delta.sourcing.model.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.{Latest, Revision}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import org.scalatest.{Assertion, CancelAfterFailure}

import java.util.UUID

class ResourcesImplSpec
    extends CatsEffectSpec
    with DoobieScalaTestFixture
    with ValidateResourceFixture
    with CancelAfterFailure
    with ConfigFixtures
    with CirceLiteral {

  implicit private val subject: Subject = Identity.User("user", Label.unsafe("realm"))
  implicit private val caller: Caller   = Caller(subject, Set(subject))

  private val uuid                  = UUID.randomUUID()
  implicit private val uuidF: UUIDF = UUIDF.fixed(uuid)

  implicit private val rcr: RemoteContextResolution =
    RemoteContextResolution.fixedIO(
      contexts.metadata        -> ContextValue.fromFile("contexts/metadata.json"),
      contexts.shacl           -> ContextValue.fromFile("contexts/shacl.json"),
      contexts.schemasMetadata -> ContextValue.fromFile("contexts/schemas-metadata.json")
    )

  private val schemaOrg         = Vocabulary.schema
  private val org               = Label.unsafe("myorg")
  private val am                = ApiMappings(Map("nxv" -> nxv.base, "Person" -> schema.Person, "schema" -> schemaOrg.base))
  private val projBase          = nxv.base
  private val project           = ProjectGen.project("myorg", "myproject", base = projBase, vocab = schemaOrg.base, mappings = am)
  private val projectDeprecated = ProjectGen.project("myorg", "myproject2")
  private val projectRef        = project.ref
  private val allApiMappings    = am + Resources.mappings

  private val schema1 = nxv + "myschema"
  private val schema2 = nxv + "myschema2"

  private val validateResource = validateFor(Set((project.ref, schema1), (project.ref, schema2)))

  private val fetchContext = FetchContextDummy(
    Map(
      project.ref           -> project.context.copy(apiMappings = allApiMappings),
      projectDeprecated.ref -> projectDeprecated.context
    ),
    Set(projectDeprecated.ref)
  )

  private val detectChanges = DetectChange(enabled = true)

  private val resolverContextResolution: ResolverContextResolution = new ResolverContextResolution(
    rcr,
    (r, p, _) => resources.fetch(r, p).attempt.map(_.left.map(_ => ResourceResolutionReport()))
  )

  private val resourceDef      = Resources.definition(validateResource, detectChanges, clock)
  private lazy val resourceLog = ScopedEventLog(resourceDef, eventLogConfig, xas)

  private lazy val resources: Resources = ResourcesImpl(
    resourceLog,
    fetchContext,
    resolverContextResolution
  )

  private val simpleSourcePaylod = (id: IdSegment) => json"""{ "@id": "$id", "some": "content" }"""

  "The Resources operations bundle" when {

    // format: off
    val myId  = nxv + "myid"  // Resource created against the resource schema with id present on the payload
    val myId3 = nxv + "myid3" // Resource created against the resource schema with id present on the payload and passed explicitly
    val myId4 = nxv + "myid4" // Resource created against schema1 with id present on the payload and passed explicitly
    val myId5 = nxv + "myid5" // Resource created against the resource schema with id passed explicitly but not present on the payload
    val myId6 = nxv + "myid6" // Resource created against schema1 with id passed explicitly but not present on the payload
    val myId7 = nxv + "myid7" // Resource created against the resource schema with id passed explicitly and with payload without @context
    val myId8  = nxv + "myid8" // Resource created against the resource schema with id present on the payload and having its context pointing on metadata and myId1 and myId2
    val myId9  = nxv + "myid9" // Resource created against the resource schema with id present on the payload and having its context pointing on metadata and myId8 so therefore myId1 and myId2
    val myId10 = nxv + "myid10"
    val myId11 = nxv + "myid11"
    val myId12 = nxv + "myid12"
    val myId13 = nxv + "myid13"

    // format: on
    val resourceSchema    = Latest(schemas.resources)
    val myId2             = nxv + "myid2" // Resource created against the schema1 with id present on the payload
    val types             = Set(schemaOrg + "Custom")
    val source            = jsonContentOf("resources/resource.json", "id" -> myId)
    def sourceWithBlankId = source deepMerge json"""{"@id": ""}"""
    val tag               = UserTag.unsafe("tag")

    def mkResource(res: Resource): DataResource =
      ResourceGen.resourceFor(res, types = types, subject = subject)

    "creating a resource" should {

      "succeed with the id present on the payload" in {
        forAll(List(myId -> resourceSchema, myId2 -> Latest(schema1))) { case (id, schemaRef) =>
          val sourceWithId = source deepMerge json"""{"@id": "$id"}"""
          val expectedData = ResourceGen.resource(id, projectRef, sourceWithId, Revision(schemaRef.iri, 1))
          val resource     = resources.create(projectRef, schemaRef, sourceWithId, None).accepted
          resource shouldEqual mkResource(expectedData)
        }
      }

      "succeed and tag with the id present on the payload" in {
        forAll(List(myId10 -> resourceSchema, myId11 -> Latest(schema1))) { case (id, schemaRef) =>
          val sourceWithId     = source deepMerge json"""{"@id": "$id"}"""
          val expectedData     =
            ResourceGen.resource(id, projectRef, sourceWithId, Revision(schemaRef.iri, 1), Tags(tag -> 1))
          val expectedResource = mkResource(expectedData)

          val resource = resources.create(projectRef, schemaRef, sourceWithId, Some(tag)).accepted

          val resourceByTag = resources.fetch(IdSegmentRef(id, tag), projectRef, Some(schemaRef)).accepted
          resource shouldEqual expectedResource
          resourceByTag shouldEqual expectedResource
        }
      }

      "succeed with the id present on the payload and passed" in {
        val list =
          List(
            (myId3, "_", resourceSchema),
            (myId4, "myschema", Latest(schema1))
          )
        forAll(list) { case (id, schemaSegment, schemaRef) =>
          val sourceWithId = source deepMerge json"""{"@id": "$id"}"""
          val expectedData = ResourceGen.resource(id, projectRef, sourceWithId, Revision(schemaRef.iri, 1))
          val resource     = resources.create(id, projectRef, schemaSegment, sourceWithId, None).accepted
          resource shouldEqual mkResource(expectedData)
        }
      }

      "succeed and tag with the id present on the payload and passed" in {
        val list =
          List(
            (myId12, "_", resourceSchema),
            (myId13, "myschema", Latest(schema1))
          )
        forAll(list) { case (id, schemaSegment, schemaRef) =>
          val sourceWithId     = source deepMerge json"""{"@id": "$id"}"""
          val expectedData     =
            ResourceGen.resource(id, projectRef, sourceWithId, Revision(schemaRef.iri, 1), Tags(tag -> 1))
          val expectedResource = mkResource(expectedData)

          val resource = resources.create(id, projectRef, schemaSegment, sourceWithId, Some(tag)).accepted

          val resourceByTag = resources.fetch(IdSegmentRef(id, tag), projectRef, Some(schemaRef)).accepted
          resource shouldEqual expectedResource
          resourceByTag shouldEqual expectedResource
        }
      }

      "succeed with the passed id" in {
        val list = List(
          ("nxv:myid5", myId5, resourceSchema),
          ("nxv:myid6", myId6, Latest(schema1))
        )
        forAll(list) { case (segment, iri, schemaRef) =>
          val sourceWithId    = source deepMerge json"""{"@id": "$iri"}"""
          val sourceWithoutId = source.removeKeys(keywords.id)
          val expectedData    =
            ResourceGen
              .resource(iri, projectRef, sourceWithId, Revision(schemaRef.iri, 1))
              .copy(source = sourceWithoutId)
          val resource        = resources.create(segment, projectRef, schemaRef, sourceWithoutId, None).accepted
          resource shouldEqual mkResource(expectedData)
        }
      }

      "succeed with payload without @context" in {
        val payload        = json"""{ "@type": "Person", "name": "Alice"}"""
        val payloadWithCtx =
          payload.addContext(json"""{"@context": {"@vocab": "${schemaOrg.base}","@base": "${nxv.base}"}}""")
        val schemaRev      = Revision(resourceSchema.iri, 1)
        val expectedData   =
          ResourceGen.resource(myId7, projectRef, payloadWithCtx, schemaRev).copy(source = payload)

        resources.create(myId7, projectRef, schemas.resources, payload, None).accepted shouldEqual
          ResourceGen.resourceFor(expectedData, Set(schema.Person), subject = subject)
      }

      "succeed with the id present on the payload and pointing to another resource in its context" in {
        val sourceMyId8  =
          source.addContext(contexts.metadata).addContext(myId).addContext(myId2) deepMerge json"""{"@id": "$myId8"}"""
        val schemaRev    = Revision(resourceSchema.iri, 1)
        val expectedData =
          ResourceGen.resource(myId8, projectRef, sourceMyId8, schemaRev)(
            resolverContextResolution(projectRef)
          )
        val resource     = resources.create(projectRef, resourceSchema, sourceMyId8, None).accepted
        resource shouldEqual mkResource(expectedData)
      }

      "succeed when pointing to another resource which itself points to other resources in its context" in {
        val sourceMyId9  = source.addContext(contexts.metadata).addContext(myId8) deepMerge json"""{"@id": "$myId9"}"""
        val schemaRev    = Revision(resourceSchema.iri, 1)
        val expectedData =
          ResourceGen.resource(myId9, projectRef, sourceMyId9, schemaRev)(
            resolverContextResolution(projectRef)
          )
        val resource     = resources.create(projectRef, resourceSchema, sourceMyId9, None).accepted
        resource shouldEqual mkResource(expectedData)
      }

      "reject with different ids on the payload and passed" in {
        val otherId = nxv + "other"
        resources.create(otherId, projectRef, schemas.resources, source, None).rejected shouldEqual
          UnexpectedId(id = otherId, payloadId = myId)
      }

      "reject if the id is blank" in {
        resources.create(projectRef, schemas.resources, sourceWithBlankId, None).rejected shouldEqual BlankId
      }

      "reject if it already exists" in {
        resources.create(myId, projectRef, schemas.resources, source, None).rejected shouldEqual
          ResourceAlreadyExists(myId, projectRef)

        resources
          .create("nxv:myid", projectRef, schemas.resources, source, None)
          .rejected shouldEqual
          ResourceAlreadyExists(myId, projectRef)
      }

      "reject if the validated schema does not exists" in {
        val otherId    = nxv + "other"
        val noIdSource = source.removeKeys(keywords.id)
        resources.create(otherId, projectRef, "nxv:notExist", noIdSource, None).rejected shouldEqual
          InvalidSchemaRejection(
            Latest(nxv + "notExist"),
            project.ref,
            ResourceResolutionReport()
          )
      }

      "reject if project does not exist" in {
        val projectRef = ProjectRef(org, Label.unsafe("other"))
        resources.create(projectRef, schemas.resources, source, None).rejectedWith[ProjectNotFound]

        resources.create(myId, projectRef, schemas.resources, source, None).rejectedWith[ProjectNotFound]
      }

      "reject if project is deprecated" in {
        resources.create(projectDeprecated.ref, schemas.resources, source, None).rejectedWith[ProjectIsDeprecated]

        resources
          .create(myId, projectDeprecated.ref, schemas.resources, source, None)
          .rejectedWith[ProjectIsDeprecated]
      }

      "reject if part of the context can't be resolved" in {
        val myIdX           = nxv + "myidx"
        val unknownResource = nxv + "fail"
        val sourceMyIdX     =
          source.addContext(contexts.metadata).addContext(unknownResource) deepMerge json"""{"@id": "$myIdX"}"""
        resources.create(projectRef, resourceSchema, sourceMyIdX, None).rejectedWith[InvalidJsonLdFormat]
      }

      "reject for an incorrect payload" in {
        val myIdX       = nxv + "myidx"
        val sourceMyIdX =
          source.addContext(
            contexts.metadata
          ) deepMerge json"""{"other": {"@id": " http://nexus.example.com/myid"}}""" deepMerge json"""{"@id": "$myIdX"}"""
        resources.create(projectRef, resourceSchema, sourceMyIdX, None).rejectedWith[InvalidJsonLdFormat]
      }
    }

    "updating a resource" should {

      "succeed" in {
        val updated      = source.removeKeys(keywords.id) deepMerge json"""{"number": 60}"""
        val expectedData = ResourceGen.resource(myId2, projectRef, updated, Revision(schema1, 1))
        val expected     = mkResource(expectedData).copy(rev = 2)
        val actual       = resources.update(myId2, projectRef, Some(schema1), 1, updated, None).accepted
        actual shouldEqual expected
      }

      "successfully tag" in {
        val updated      = source.removeKeys(keywords.id) deepMerge json"""{"number": 60}"""
        val newTag       = UserTag.unsafe(genString())
        val expectedData =
          ResourceGen.resource(myId10, projectRef, updated, Revision(schema1, 1), tags = Tags(tag -> 1, newTag -> 2))
        val expected     = mkResource(expectedData).copy(rev = 2)
        val actual       = resources.update(myId10, projectRef, Some(schema1), 1, updated, Some(newTag)).accepted
        val byTag        = resources.fetch(IdSegmentRef(myId10, newTag), projectRef, None).accepted
        actual shouldEqual expected
        byTag shouldEqual expected
      }

      "succeed without specifying the schema" in {
        val updated      = source.removeKeys(keywords.id) deepMerge json"""{"number": 65}"""
        val expectedData = ResourceGen.resource(myId2, projectRef, updated, Revision(schema1, 1))
        resources.update("nxv:myid2", projectRef, None, 2, updated, None).accepted shouldEqual
          mkResource(expectedData).copy(rev = 3)
      }

      "succeed when changing the schema" in {
        val updatedSource   = source.removeKeys(keywords.id) deepMerge json"""{"number": 70}"""
        val newSchema       = Revision(schema2, 1)
        val updatedResource = resources.update(myId2, projectRef, Some(newSchema.iri), 3, updatedSource, None).accepted

        updatedResource.rev shouldEqual 4
        updatedResource.schema shouldEqual newSchema
      }

      "be skipped and return the current resource when the update does not introduce a change" in {
        val (before, after) = {
          for {
            current <- resources.fetch(myId2, projectRef, None)
            updated <- resources.update(myId2, projectRef, None, current.rev, current.value.source, None)
          } yield (current, updated)
        }.accepted

        after shouldEqual before
      }

      "reject if it doesn't exists" in {
        resources
          .update(nxv + "other", projectRef, None, 1, json"""{"a": "b"}""", None)
          .rejectedWith[ResourceNotFound]
      }

      "reject if the revision passed is incorrect" in {
        resources.update(myId, projectRef, None, 3, json"""{"a": "b"}""", None).rejected shouldEqual
          IncorrectRev(provided = 3, expected = 1)
      }

      "reject if deprecated" in {
        resources.deprecate(myId3, projectRef, None, 1).accepted
        resources
          .update(myId3, projectRef, None, 2, json"""{"a": "b"}""", None)
          .rejectedWith[ResourceIsDeprecated]
        resources
          .update("nxv:myid3", projectRef, None, 2, json"""{"a": "b"}""", None)
          .rejectedWith[ResourceIsDeprecated]
      }

      "reject if the validated schema does not exists" in {
        val otherId    = nxv + "other"
        val noIdSource = source.removeKeys(keywords.id)
        resources.update(myId2, projectRef, Some(otherId), 4, noIdSource, None).rejectedWith[InvalidSchemaRejection]
      }

      "reject if project does not exist" in {
        val projectRef = ProjectRef(org, Label.unsafe("other"))

        resources.update(myId, projectRef, None, 2, source, None).rejectedWith[ProjectNotFound]
      }

      "reject if project is deprecated" in {
        resources.update(myId, projectDeprecated.ref, None, 2, source, None).rejectedWith[ProjectIsDeprecated]
      }
    }

    "refreshing a resource" should {
      val schema               = schemas.resources
      val contextId            = nxv + "context"
      val contextSource        = json""" { "@context": { "@base": "http://bbp.epfl.ch/base/" } }"""
      val contextUpdatedSource =
        json""" { "@context": { "@base": "http://example.com/base/", "prefix": "http://bbp.epfl.ch/prefix" } }"""
      val idRefresh            = nxv + "refresh"

      val refreshSource = json"""{
                                    "@context": [
                                      "$contextId",
                                      { "@vocab": "${schemaOrg.base}" }
                                    ],
                                    "@type": "Person",
                                    "name": "Alex"
                                 }"""

      "succeed" in {
        val io = for {
          _                      <- resources.create(contextId, projectRef, schema, contextSource, None)
          createdResult          <- resources.create(idRefresh, projectRef, schema, refreshSource, None)
          refreshedNoChange      <- resources.refresh(idRefresh, projectRef, None)
          _                       = refreshedNoChange shouldEqual createdResult
          updatedContext         <- resources.update(contextId, projectRef, None, 1, contextUpdatedSource, None)
          _                       = updatedContext.rev shouldEqual 2
          refreshedContextChange <- resources.refresh(idRefresh, projectRef, None)
          _                       = refreshedContextChange shouldEqual createdResult.copy(rev = 2)
        } yield ()
        io.accepted
      }

      "reject if it doesn't exists" in {
        resources
          .refresh(nxv + "other", projectRef, None)
          .rejectedWith[ResourceNotFound]
      }

      "reject if schemas do not match" in {
        resources
          .refresh(idRefresh, projectRef, Some(schema1))
          .rejectedWith[UnexpectedResourceSchema]
      }

      "reject if project does not exist" in {
        val projectRef = ProjectRef(org, Label.unsafe("other"))

        resources.refresh(idRefresh, projectRef, None).rejectedWith[ProjectNotFound]
      }

      "reject if project is deprecated" in {
        resources.refresh(idRefresh, projectDeprecated.ref, None).rejectedWith[ProjectIsDeprecated]
      }

      "reject if deprecated" in {
        resources.deprecate(idRefresh, projectRef, None, 2).accepted
        resources
          .refresh(idRefresh, projectRef, None)
          .rejectedWith[ResourceIsDeprecated]
        resources
          .refresh("nxv:refresh", projectRef, None)
          .rejectedWith[ResourceIsDeprecated]
      }
    }

    "updating a resource schema" should {

      val id           = nxv + "schemaUpdate"
      val sourceWithId = source deepMerge json"""{"@id": "$id"}"""

      val nonExistentResourceId = iri"http://does.not.exist"
      val nonExistentSchemaId   = iri"http://does.not.exist"
      val nonExistentProject    = ProjectRef(Label.unsafe("not"), Label.unsafe("there"))

      "reject if the resource doesn't exist" in {
        resources
          .updateAttachedSchema(nonExistentResourceId, projectRef, schema2)
          .rejectedWith[ResourceNotFound]
      }

      "reject if the schema doesn't exist" in {
        resources.create(id, projectRef, schema1, sourceWithId, None).accepted
        resources
          .updateAttachedSchema(id, projectRef, nonExistentSchemaId)
          .rejectedWith[InvalidSchemaRejection]
      }

      "reject if the project doesn't exist" in {
        resources
          .updateAttachedSchema(id, nonExistentProject, schema2)
          .rejectedWith[ProjectNotFound]
      }

      "succeed" in {
        val updated = resources.updateAttachedSchema(id, projectRef, schema2).accepted
        updated.schema.iri shouldEqual schema2

        val fetched = resources.fetch(id, projectRef, None).accepted
        fetched.schema.iri shouldEqual schema2
      }

    }

    "tagging a resource" should {

      "succeed" in {
        val schemaRev          = Revision(resourceSchema.iri, 1)
        val expectedData       = ResourceGen.resource(myId, projectRef, source, schemaRev, tags = Tags(tag -> 1))
        val expectedLatestRev  = mkResource(expectedData).copy(rev = 2)
        val expectedTaggedData = expectedData.copy(tags = Tags.empty)
        val expectedTaggedRev  = mkResource(expectedTaggedData).copy(rev = 1)

        val resource =
          resources.tag(myId, projectRef, Some(schemas.resources), tag, 1, 1).accepted

        // Lookup by tag should return the tagged rev but have no tags in the data
        val taggedRevision =
          resources.fetch(IdSegmentRef(myId, tag), projectRef, Some(schemas.resources)).accepted
        // Lookup by latest revision should return rev 2 with tags in the data
        val latestRevision =
          resources.fetch(ResourceRef(myId), projectRef).accepted

        resource shouldEqual expectedLatestRev
        latestRevision shouldEqual expectedLatestRev
        taggedRevision shouldEqual expectedTaggedRev
      }

      "reject if it doesn't exists" in {
        resources.tag(nxv + "other", projectRef, None, tag, 1, 1).rejectedWith[ResourceNotFound]
      }

      "reject if the revision passed is incorrect" in {
        resources.tag(myId, projectRef, None, tag, 1, 3).rejected shouldEqual
          IncorrectRev(provided = 3, expected = 2)
      }

      "succeed if deprecated" in {
        resources.tag(myId3, projectRef, None, tag, 2, 2).accepted
      }

      "reject if schemas do not match" in {
        resources
          .tag(myId2, projectRef, Some(schemas.resources), tag, 2, 4)
          .rejectedWith[UnexpectedResourceSchema]
      }

      "reject if tag revision not found" in {
        resources
          .tag(myId, projectRef, Some(schemas.resources), tag, 6, 2)
          .rejected shouldEqual
          RevisionNotFound(provided = 6, current = 2)
      }

      "reject if project does not exist" in {
        val projectRef = ProjectRef(org, Label.unsafe("other"))

        resources.tag(myId, projectRef, None, tag, 2, 1).rejectedWith[ProjectNotFound]
      }

      "reject if project is deprecated" in {
        resources.tag(myId, projectDeprecated.ref, None, tag, 2, 1).rejectedWith[ProjectIsDeprecated]
      }
    }

    "deprecating a resource" should {

      "succeed" in {
        val sourceWithId = source deepMerge json"""{"@id": "$myId4"}"""
        val expectedData = ResourceGen.resource(myId4, projectRef, sourceWithId, Revision(schema1, 1))
        val resource     = resources.deprecate(myId4, projectRef, Some(schema1), 1).accepted
        resource shouldEqual mkResource(expectedData).copy(rev = 2, deprecated = true)
        assertDeprecated(myId4, projectRef)
      }

      "reject if it doesn't exists" in {
        resources.deprecate(nxv + "other", projectRef, None, 1).rejectedWith[ResourceNotFound]
      }

      "reject if the revision passed is incorrect" in {
        givenAResource { id =>
          resources
            .deprecate(id, projectRef, None, 3)
            .assertRejectedEquals(IncorrectRev(3, 1))
          assertRemainsActive(id, projectRef)
        }
      }

      "reject if deprecated" in {
        givenADeprecatedResource { id =>
          resources.deprecate(id, projectRef, None, 2).assertRejectedWith[ResourceIsDeprecated]
          assertRemainsDeprecated(id, projectRef)
        }
      }

      "reject if schemas do not match" in {
        givenAResource { id =>
          resources.deprecate(id, projectRef, Some(schema1), 1).assertRejectedWith[UnexpectedResourceSchema]
          assertRemainsActive(id, projectRef)
        }
      }

      "reject if project does not exist" in {
        givenAResource { id =>
          val wrongProject = ProjectRef(org, Label.unsafe("other"))
          resources.deprecate(id, wrongProject, None, 1).assertRejectedWith[ProjectNotFound]
        }
      }

      "reject if project is deprecated" in {
        resources.deprecate(nxv + "id", projectDeprecated.ref, None, 1).assertRejectedWith[ProjectIsDeprecated]
      }

    }

    "undeprecating a resource" should {

      "succeed" in {
        givenADeprecatedResource { id =>
          resources.undeprecate(id, projectRef, None, 2).accepted.deprecated shouldEqual false
          assertActive(id, projectRef)
        }
      }

      "reject if resource does not exist" in {
        givenADeprecatedResource { _ =>
          val wrongId = nxv + genString()
          resources
            .undeprecate(wrongId, projectRef, None, 2)
            .assertRejectedWith[ResourceNotFound]
        }
      }

      "reject if resource is revision is incorrect" in {
        givenADeprecatedResource { id =>
          resources
            .undeprecate(id, projectRef, None, 4)
            .assertRejectedWith[IncorrectRev]
          assertRemainsDeprecated(id, projectRef)
        }
      }

      "reject if resource is not deprecated" in {
        givenAResource { id =>
          resources
            .undeprecate(id, projectRef, None, 1)
            .assertRejectedWith[ResourceIsNotDeprecated]
          assertRemainsActive(id, projectRef)
        }
      }

      "reject if schema does not match" in {
        givenADeprecatedResource { id =>
          resources
            .undeprecate(id, projectRef, Some(schema1), 2)
            .assertRejectedWith[UnexpectedResourceSchema]
          assertRemainsDeprecated(id, projectRef)
        }
      }

      "reject if project does not exist" in {
        givenADeprecatedResource { id =>
          val wrongProject = ProjectRef.unsafe(genString(), genString())
          resources
            .undeprecate(id, wrongProject, None, 2)
            .assertRejectedWith[ProjectNotFound]
        }
      }

    }

    "fetching a resource" should {
      val schemaRev          = Revision(resourceSchema.iri, 1)
      val expectedData       = ResourceGen.resource(myId, projectRef, source, schemaRev)
      val expectedDataLatest = expectedData.copy(tags = Tags(tag -> 1))

      "succeed" in {
        forAll(List[Option[IdSegment]](None, Some(schemas.resources))) { schema =>
          resources.fetch(myId, projectRef, schema).accepted shouldEqual
            mkResource(expectedDataLatest).copy(rev = 2)
        }
      }

      "succeed by tag" in {
        forAll(List[Option[IdSegment]](None, Some(schemas.resources))) { schema =>
          resources.fetch(IdSegmentRef("nxv:myid", tag), projectRef, schema).accepted shouldEqual
            mkResource(expectedData)
        }
      }

      "succeed by rev" in {
        forAll(List[Option[IdSegment]](None, Some(schemas.resources))) { schema =>
          resources.fetch(IdSegmentRef(myId, 1), projectRef, schema).accepted shouldEqual
            mkResource(expectedData)
        }
      }

      "reject if tag does not exist" in {
        val otherTag = UserTag.unsafe("other")
        forAll(List[Option[IdSegment]](None, Some(schemas.resources))) { schema =>
          resources.fetch(IdSegmentRef(myId, otherTag), projectRef, schema).rejected shouldEqual TagNotFound(otherTag)
        }
      }

      "reject if revision does not exist" in {
        forAll(List[Option[IdSegment]](None, Some(schemas.resources))) { schema =>
          resources.fetch(IdSegmentRef(myId, 5), projectRef, schema).rejected shouldEqual
            RevisionNotFound(provided = 5, current = 2)
        }
      }

      "fail fetching if resource does not exist" in {
        val myId = nxv + "notFound"
        resources.fetch(myId, projectRef, None).rejectedWith[ResourceNotFound]
        resources.fetch(IdSegmentRef(myId, tag), projectRef, None).rejectedWith[ResourceNotFound]
        resources.fetch(IdSegmentRef(myId, 2), projectRef, None).rejectedWith[ResourceNotFound]
      }

      "fail fetching if schema is not resource schema" in {
        resources.fetch(myId, projectRef, Some(schema1)).rejectedWith[ResourceNotFound]
        resources.fetch(IdSegmentRef(myId, tag), projectRef, Some(schema1)).rejectedWith[ResourceNotFound]
        resources.fetch(IdSegmentRef(myId, 2), projectRef, Some(schema1)).rejectedWith[ResourceNotFound]
      }

      "reject if project does not exist" in {
        val projectRef = ProjectRef(org, Label.unsafe("other"))

        resources.fetch(myId, projectRef, None).rejectedWith[ProjectNotFound]
      }

      "fail fetching if resource does not exist on deprecated project" in {
        resources.fetch(myId, projectDeprecated.ref, None).rejectedWith[ResourceNotFound]
      }
    }

    "deleting a tag" should {
      "succeed" in {
        val schemaRev    = Revision(resourceSchema.iri, 1)
        val expectedData = ResourceGen.resource(myId, projectRef, source, schemaRev)
        val resource     =
          resources.deleteTag(myId, projectRef, Some(schemas.resources), tag, 2).accepted
        resource shouldEqual mkResource(expectedData).copy(rev = 3)
      }

      "reject if the resource doesn't exists" in {
        resources.deleteTag(nxv + "other", projectRef, None, tag, 1).rejectedWith[ResourceNotFound]
      }

      "reject if the revision passed is incorrect" in {
        resources.deleteTag(myId, projectRef, None, tag, 4).rejected shouldEqual
          IncorrectRev(provided = 4, expected = 3)
      }

      "reject if schemas do not match" in {
        resources
          .deleteTag(myId2, projectRef, Some(schemas.resources), tag, 4)
          .rejectedWith[UnexpectedResourceSchema]
      }

      "reject if the tag doesn't exist" in {
        resources.deleteTag(myId, projectRef, Some(schemas.resources), tag, 3).rejectedWith[TagNotFound]
      }
    }

    "deleting a resource" should {
      "succeed for an existing resource" in {
        resources.delete(myId, projectRef).accepted
        resources.fetch(myId, projectRef, None).rejectedWith[ResourceNotFound]
      }

      "reject if it doesn't exists" in {
        resources
          .delete(nxv + "xxx", projectRef)
          .rejectedWith[ResourceNotFound]
      }

      "reject if project is deprecated" in {
        resources.delete(nxv + "xxx", projectDeprecated.ref).assertRejectedWith[ProjectIsDeprecated]
      }
    }

    /** Provides a newly created resource for assertion. Latest revision is 1 */
    def givenAResource(assertion: IdSegment => Assertion): Assertion = {
      val id = nxv + genString()
      resources.create(id, projectRef, resourceSchema, simpleSourcePaylod(id), None).accepted
      resources.fetch(id, projectRef, None).accepted
      assertion(id)
    }

    /** Provides a deprecated resource for assertion. Latest revision is 2 */
    def givenADeprecatedResource(assertion: IdSegment => Assertion): Assertion =
      givenAResource { id =>
        resources.deprecate(id, projectRef, Some(schemas.resources), 1).accepted.deprecated shouldEqual true
        resources.fetch(id, projectRef, None).accepted.deprecated shouldEqual true
        assertion(id)
      }

    def assertDeprecated(id: IdSegment, projectRef: ProjectRef, schema: Option[IdSegment] = None)        =
      resources.fetch(id, projectRef, schema).accepted.deprecated shouldEqual true
    def assertRemainsDeprecated(id: IdSegment, projectRef: ProjectRef, schema: Option[IdSegment] = None) =
      assertDeprecated(id, projectRef, schema)
    def assertActive(id: IdSegment, projectRef: ProjectRef, schema: Option[IdSegment] = None)            =
      resources.fetch(id, projectRef, schema).accepted.deprecated shouldEqual false
    def assertRemainsActive(id: IdSegment, projectRef: ProjectRef, schema: Option[IdSegment] = None)     =
      assertActive(id, projectRef, schema)
  }
}
