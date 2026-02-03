package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.cache.LocalCache
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv, schema, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
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
import ai.senscience.nexus.delta.sdk.{ConfigFixtures, DataResource, RemoteContextResolutionFixtures}
import ai.senscience.nexus.delta.sourcing.ScopedEventLog
import ai.senscience.nexus.delta.sourcing.model.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.{Latest, Revision}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import munit.{AnyFixture, Location}

import java.util.UUID

class ResourcesImplSuite
    extends NexusSuite
    with Doobie.Fixture
    with ValidateResourceFixture
    with ConfigFixtures
    with RemoteContextResolutionFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private given subject: Subject = Identity.User("user", Label.unsafe("realm"))
  private given caller: Caller   = Caller(subject, Set(subject))

  private val uuid    = UUID.randomUUID()
  private given UUIDF = UUIDF.fixed(uuid)

  private given rcr: RemoteContextResolution = loadCoreContextsAndSchemas

  private val schemaOrg         = Vocabulary.schema
  private val am                = ApiMappings(Map("nxv" -> nxv.base, "Person" -> schema.Person, "schema" -> schemaOrg.base))
  private val projBase          = nxv.base
  private val project           = ProjectGen.project("myorg", "myproject", base = projBase, vocab = schemaOrg.base, mappings = am)
  private val projectDeprecated = ProjectGen.project("myorg", "myproject2")
  private val unknownProject    = ProjectRef.unsafe("myorg", "unknown")
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
    LocalCache.noop,
    (r, p, _) => resources.fetch(r, p).attempt.map(_.left.map(_ => ResourceResolutionReport()))
  )

  private val resourceDef      = Resources.definition(validateResource, detectChanges, clock)
  private lazy val resourceLog = ScopedEventLog(resourceDef, eventLogConfig, xas)

  private lazy val resources: Resources = ResourcesImpl(
    resourceLog,
    fetchContext,
    resolverContextResolution
  )

  private val unconstrained: Iri = schemas.resources
  private val unconstrainedRef   = Latest(unconstrained)
  private val unconstrainedRev   = Revision(unconstrained, 1)
  private val types              = Set(schemaOrg + "Custom")
  private val tag                = UserTag.unsafe("tag")

  private def genId(): Iri                 = nxv + genString()
  private def nxvSegment(id: Iri): String  = s"nxv:${id.lastSegment.getOrElse("unknown")}"
  private lazy val source: Json            = jsonContentOf("resources/resource.json", "id" -> "placeholder")
  private def sourceWithId(id: Iri): Json  = source.deepMerge(json"""{"@id": "$id"}""")
  private def sourceWithoutId: Json        = source.removeKeys(keywords.id)
  private def sourceWithBlankId: Json      = source.deepMerge(json"""{"@id": ""}""")
  private def simpleSourcePayload(id: Iri) = json"""{ "@id": "$id", "some": "content" }"""

  private def mkResource(res: Resource): DataResource =
    ResourceGen.resourceFor(res, types = types, subject = subject)

  // -----------------------------------------
  // Creating a resource
  // -----------------------------------------

  test("Creating a resource succeeds with the id present on the payload") {
    (unconstrainedRef, Latest(schema1)).traverse { schemaRef =>
      val id           = genId()
      val payload      = sourceWithId(id)
      val expectedData = ResourceGen.resource(id, projectRef, payload, Revision(schemaRef.iri, 1))
      resources
        .create(projectRef, schemaRef, payload, None)
        .assertEquals(mkResource(expectedData), s"for schema $schemaRef")
    }
  }

  test("Creating a resource succeeds and tags with the id present on the payload") {
    (unconstrainedRef, Latest(schema1)).traverse { schemaRef =>
      val id               = genId()
      val payload          = sourceWithId(id)
      val expectedData     = ResourceGen.resource(id, projectRef, payload, Revision(schemaRef.iri, 1), Tags(tag -> 1))
      val expectedResource = mkResource(expectedData)
      val clue             = s"for schema $schemaRef"

      for {
        _ <- resources.create(projectRef, schemaRef, payload, Some(tag)).assertEquals(expectedResource, clue)
        _ <- resources.fetch(IdSegmentRef(id, tag), projectRef, Some(schemaRef)).assertEquals(expectedResource, clue)
      } yield ()
    }
  }

  test("Creating a resource succeeds with the id present on the payload and passed") {
    (("_", unconstrainedRef), ("myschema", Latest(schema1))).traverse { case (schemaSegment, schemaRef) =>
      val id           = genId()
      val payload      = sourceWithId(id)
      val expectedData = ResourceGen.resource(id, projectRef, payload, Revision(schemaRef.iri, 1))
      resources
        .create(id, projectRef, schemaSegment, payload, None)
        .assertEquals(mkResource(expectedData), s"for schema $schemaRef")
    }
  }

  test("Creating a resource succeeds and tags with the id present on the payload and passed") {
    (("_", unconstrainedRef), ("myschema", Latest(schema1))).traverse { case (schemaSegment, schemaRef) =>
      val id               = genId()
      val payload          = sourceWithId(id)
      val expectedData     =
        ResourceGen.resource(id, projectRef, payload, Revision(schemaRef.iri, 1), Tags(tag -> 1))
      val expectedResource = mkResource(expectedData)
      val clue             = s"for schema $schemaRef"

      for {
        _ <- resources.create(id, projectRef, schemaSegment, payload, Some(tag)).assertEquals(expectedResource, clue)
        _ <- resources.fetch(IdSegmentRef(id, tag), projectRef, Some(schemaRef)).assertEquals(expectedResource, clue)
      } yield ()
    }
  }

  test("Creating a resource succeeds with the passed id") {
    (unconstrainedRef, Latest(schema1)).traverse { schemaRef =>
      val id            = genId()
      val segment       = nxvSegment(id)
      val payloadWithId = sourceWithId(id)
      val expectedData  =
        ResourceGen
          .resource(id, projectRef, payloadWithId, Revision(schemaRef.iri, 1))
          .copy(source = sourceWithoutId)
      resources
        .create(segment, projectRef, schemaRef, sourceWithoutId, None)
        .assertEquals(mkResource(expectedData), s"for schema $schemaRef")
    }
  }

  test("Creating a resource succeeds with payload without @context") {
    val id             = genId()
    val payload        = json"""{ "@type": "Person", "name": "Alice"}"""
    val payloadWithCtx =
      payload.addContext(json"""{"@context": {"@vocab": "${schemaOrg.base}","@base": "${nxv.base}"}}""")
    val expectedData   =
      ResourceGen.resource(id, projectRef, payloadWithCtx, unconstrainedRev).copy(source = payload)

    resources
      .create(id, projectRef, unconstrained, payload, None)
      .assertEquals(
        ResourceGen.resourceFor(expectedData, Set(schema.Person), subject = subject)
      )
  }

  test(
    "Creating a resource succeeds with the id present on the payload and pointing to another resource in its context"
  ) {
    val contextId1 = genId()
    val contextId2 = genId()
    val id         = genId()
    for {
      // Create two resources to be used as contexts
      _           <- resources.create(contextId1, projectRef, unconstrained, sourceWithId(contextId1), None)
      _           <- resources.create(contextId2, projectRef, schema1, sourceWithId(contextId2), None)
      // Create resource pointing to them in context
      payload      = source
                       .addContext(contexts.metadata)
                       .addContext(contextId1)
                       .addContext(contextId2)
                       .deepMerge(json"""{"@id": "$id"}""")
      expectedData =
        ResourceGen.resource(id, projectRef, payload, unconstrainedRev)(using resolverContextResolution(projectRef))
      _           <- resources.create(projectRef, unconstrainedRef, payload, None).assertEquals(mkResource(expectedData))
    } yield ()
  }

  test(
    "Creating a resource succeeds when pointing to another resource which itself points to other resources in its context"
  ) {
    val contextId1 = genId()
    val contextId2 = genId()
    val contextId3 = genId()
    val id         = genId()
    for {
      // Create two base context resources
      _           <- resources.create(contextId1, projectRef, unconstrained, sourceWithId(contextId1), None)
      _           <- resources.create(contextId2, projectRef, schema1, sourceWithId(contextId2), None)
      // Create a resource pointing to them
      payload3     = source
                       .addContext(contexts.metadata)
                       .addContext(contextId1)
                       .addContext(contextId2)
                       .deepMerge(json"""{"@id": "$contextId3"}""")
      _           <- resources.create(projectRef, unconstrainedRef, payload3, None)
      // Create resource pointing to contextId3 (which itself points to contextId1 and contextId2)
      payload      = source.addContext(contexts.metadata).addContext(contextId3).deepMerge(json"""{"@id": "$id"}""")
      expectedData =
        ResourceGen.resource(id, projectRef, payload, unconstrainedRev)(using resolverContextResolution(projectRef))
      _           <- resources.create(projectRef, unconstrainedRef, payload, None).assertEquals(mkResource(expectedData))
    } yield ()
  }

  test("Creating a resource fails with different ids on the payload and passed") {
    val passedId  = genId()
    val payloadId = genId()
    val payload   = sourceWithId(payloadId)
    resources
      .create(passedId, projectRef, unconstrained, payload, None)
      .interceptEquals(
        UnexpectedId(id = passedId, payloadId = payloadId)
      )
  }

  test("Creating a resource fails if the id is blank") {
    resources.create(projectRef, unconstrained, sourceWithBlankId, None).interceptEquals(BlankId)
  }

  test("Creating a resource fails if it already exists") {
    val id            = genId()
    val segment       = nxvSegment(id)
    val source        = sourceWithId(id)
    val expectedError = ResourceAlreadyExists(id, projectRef)
    for {
      _ <- resources.create(id, projectRef, unconstrained, source, None)
      _ <- resources.create(id, projectRef, unconstrained, source, None).interceptEquals(expectedError)
      _ <- resources.create(segment, projectRef, unconstrained, source, None).interceptEquals(expectedError)
    } yield ()
  }

  test("Creating a resource fails if the validated schema does not exist") {
    val id            = genId()
    val expectedError = InvalidSchemaRejection(Latest(nxv + "notExist"), project.ref, ResourceResolutionReport())
    resources.create(id, projectRef, "nxv:notExist", sourceWithoutId, None).interceptEquals(expectedError)
  }

  test("Creating a resource fails if project does not exist") {
    val id     = genId()
    val source = sourceWithId(id)
    for {
      _ <- resources.create(unknownProject, unconstrained, source, None).intercept[ProjectNotFound]
      _ <- resources.create(id, unknownProject, unconstrained, source, None).intercept[ProjectNotFound]
    } yield ()
  }

  test("Creating a resource fails if project is deprecated") {
    val id     = genId()
    val source = sourceWithId(id)
    for {
      _ <- resources.create(projectDeprecated.ref, unconstrained, source, None).intercept[ProjectIsDeprecated]
      _ <- resources.create(id, projectDeprecated.ref, unconstrained, source, None).intercept[ProjectIsDeprecated]
    } yield ()
  }

  test("Creating a resource fails if part of the context can't be resolved") {
    val id              = genId()
    val unknownResource = nxv + "fail"
    val payload         = source.addContext(contexts.metadata).addContext(unknownResource).deepMerge(json"""{"@id": "$id"}""")
    resources.create(projectRef, unconstrainedRef, payload, None).intercept[InvalidJsonLdFormat]
  }

  test("Creating a resource fails for an incorrect payload") {
    val id      = genId()
    val payload = source
      .addContext(contexts.metadata)
      .deepMerge(
        json"""{"other": {"@id": " http://nexus.example.com/myid"}}""".deepMerge(json"""{"@id": "$id"}""")
      )
    resources.create(projectRef, unconstrainedRef, payload, None).intercept[InvalidJsonLdFormat]
  }

  // -----------------------------------------
  // Updating a resource
  // -----------------------------------------

  test("Updating a resource succeeds") {
    givenAResource(Latest(schema1)) { id =>
      val updated      = sourceWithoutId.deepMerge(json"""{"number": 60}""")
      val expectedData = ResourceGen.resource(id, projectRef, updated, Revision(schema1, 1))
      val expected     = mkResource(expectedData).copy(rev = 2)
      resources.update(id, projectRef, Some(schema1), 1, updated, None).assertEquals(expected)
    }
  }

  test("Updating a resource successfully tags") {
    val newTag = UserTag.unsafe(genString())
    givenATaggedResource(Latest(schema1)) { id =>
      val updated      = sourceWithoutId.deepMerge(json"""{"number": 60}""")
      val expectedData =
        ResourceGen.resource(id, projectRef, updated, Revision(schema1, 1), tags = Tags(tag -> 1, newTag -> 3))
      val expected     = mkResource(expectedData).copy(rev = 3)
      for {
        _ <- resources.update(id, projectRef, Some(schema1), 2, updated, Some(newTag)).assertEquals(expected)
        _ <- resources.fetch(IdSegmentRef(id, newTag), projectRef, None).assertEquals(expected)
      } yield ()
    }
  }

  test("Updating a resource succeeds without specifying the schema") {
    givenAResource(Latest(schema1)) { id =>
      val updated      = sourceWithoutId.deepMerge(json"""{"number": 65}""")
      val expectedData = ResourceGen.resource(id, projectRef, updated, Revision(schema1, 1))
      val segment      = nxvSegment(id)
      for {
        _ <-
          resources.update(id, projectRef, Some(schema1), 1, sourceWithoutId.deepMerge(json"""{"number": 60}"""), None)
        _ <- resources
               .update(segment, projectRef, None, 2, updated, None)
               .assertEquals(mkResource(expectedData).copy(rev = 3))
      } yield ()
    }
  }

  test("Updating a resource succeeds when changing the schema") {
    givenAResource(Latest(schema1)) { id =>
      val updatedSource = sourceWithoutId.deepMerge(json"""{"number": 70}""")
      val newSchema     = Revision(schema2, 1)
      for {
        _ <- resources.update(id, projectRef, Some(newSchema.iri), 1, updatedSource, None).map(_.rev).assertEquals(2)
        _ <- resources.fetch(id, projectRef, None).map(_.schema).assertEquals(newSchema)
      } yield ()
    }
  }

  test("Updating a resource is skipped and returns the current resource when the update does not introduce a change") {
    givenAResource(Latest(schema1)) { id =>
      for {
        current <- resources.fetch(id, projectRef, None)
        _       <- resources.update(id, projectRef, None, current.rev, current.value.source, None).assertEquals(current)
      } yield ()
    }
  }

  test("Updating a resource fails if it doesn't exist") {
    resources.update(nxv + "other", projectRef, None, 1, json"""{"a": "b"}""", None).intercept[ResourceNotFound]
  }

  test("Updating a resource fails if the revision passed is incorrect") {
    val expectedError = IncorrectRev(provided = 3, expected = 1)
    givenAResource() { id =>
      resources.update(id, projectRef, None, 3, json"""{"a": "b"}""", None).interceptEquals(expectedError)
    }
  }

  test("Updating a resource fails if deprecated") {
    givenADeprecatedResource() { id =>
      val segment = nxvSegment(id)
      for {
        _ <- resources.update(id, projectRef, None, 2, json"""{"a": "b"}""", None).intercept[ResourceIsDeprecated]
        _ <- resources.update(segment, projectRef, None, 2, json"""{"a": "b"}""", None).intercept[ResourceIsDeprecated]
      } yield ()
    }
  }

  test("Updating a resource fails if the validated schema does not exist") {
    val otherId = nxv + "other"
    givenAResource(Latest(schema1)) { id =>
      resources.update(id, projectRef, Some(otherId), 1, sourceWithoutId, None).intercept[InvalidSchemaRejection]
    }
  }

  test("Updating a resource fails if project does not exist") {
    val id = genId()
    resources.update(id, unknownProject, None, 2, sourceWithoutId, None).intercept[ProjectNotFound]
  }

  test("Updating a resource fails if project is deprecated") {
    val id = genId()
    resources.update(id, projectDeprecated.ref, None, 2, sourceWithoutId, None).intercept[ProjectIsDeprecated]
  }

  // -----------------------------------------
  // Refreshing a resource
  // -----------------------------------------

  test("Refreshing a resource succeeds") {
    val contextId            = genId()
    val idRefresh            = genId()
    val contextSource        = json""" { "@context": { "@base": "http://bbp.epfl.ch/base/" } }"""
    val contextUpdatedSource =
      json""" { "@context": { "@base": "http://example.com/base/", "prefix": "http://bbp.epfl.ch/prefix" } }"""
    val refreshSource        = json"""{
                                      "@context": [
                                        "$contextId",
                                        { "@vocab": "${schemaOrg.base}" }
                                      ],
                                      "@type": "Person",
                                      "name": "Alex"
                                   }"""

    for {
      _             <- resources.create(contextId, projectRef, unconstrained, contextSource, None)
      createdResult <- resources.create(idRefresh, projectRef, unconstrained, refreshSource, None)
      _             <- resources.refresh(idRefresh, projectRef, None).assertEquals(createdResult)
      _             <- resources.update(contextId, projectRef, None, 1, contextUpdatedSource, None).map(_.rev).assertEquals(2)
      _             <- resources.refresh(idRefresh, projectRef, None).assertEquals(createdResult.copy(rev = 2))
    } yield ()
  }

  test("Refreshing a resource fails if it doesn't exist") {
    resources.refresh(nxv + "other", projectRef, None).intercept[ResourceNotFound]
  }

  test("Refreshing a resource fails if schemas do not match") {
    givenAResource() { id =>
      resources.refresh(id, projectRef, Some(schema1)).intercept[UnexpectedResourceSchema]
    }
  }

  test("Refreshing a resource fails if project does not exist") {
    resources.refresh(genId(), ProjectRef.unsafe("xxx", "other"), None).intercept[ProjectNotFound]
  }

  test("Refreshing a resource fails if project is deprecated") {
    resources.refresh(genId(), projectDeprecated.ref, None).intercept[ProjectIsDeprecated]
  }

  test("Refreshing a resource fails if deprecated") {
    givenADeprecatedResource() { id =>
      val segment = nxvSegment(id)
      resources.refresh(id, projectRef, None).intercept[ResourceIsDeprecated] >>
        resources.refresh(segment, projectRef, None).intercept[ResourceIsDeprecated]
    }
  }

  // -----------------------------------------
  // Updating a resource schema
  // -----------------------------------------

  private val nonExistentResourceId = iri"https://senscience/not/found"
  private val nonExistentSchemaId   = iri"https://senscience/not/found"
  private val nonExistentProject    = ProjectRef(Label.unsafe("not"), Label.unsafe("there"))

  test("Updating a resource schema fails if the resource doesn't exist") {
    resources.updateAttachedSchema(nonExistentResourceId, projectRef, schema2).intercept[ResourceNotFound]
  }

  test("Updating a resource schema fails if the schema doesn't exist") {
    givenAResource(Latest(schema1)) { id =>
      resources.updateAttachedSchema(id, projectRef, nonExistentSchemaId).intercept[InvalidSchemaRejection]
    }
  }

  test("Updating a resource schema fails if the project doesn't exist") {
    resources.updateAttachedSchema(genId(), nonExistentProject, schema2).intercept[ProjectNotFound]
  }

  test("Updating a resource schema succeeds") {
    givenAResource(Latest(schema1)) { id =>
      resources
        .updateAttachedSchema(id, projectRef, schema2)
        .map(_.schema.iri)
        .assertEquals(schema2)
    }
  }

  // -----------------------------------------
  // Tagging a resource
  // -----------------------------------------

  test("Tagging a resource succeeds") {
    givenAResource() { id =>
      for {
        _ <- resources.tag(id, projectRef, Some(unconstrained), tag, 1, 1).map(_.rev).assertEquals(2)
        _ <- resources.fetch(IdSegmentRef(id, tag), projectRef, Some(unconstrained)).map(_.rev).assertEquals(1)
        _ <-
          resources.fetch(ResourceRef(id), projectRef).map(r => (r.rev, r.value.tags)).assertEquals((2, Tags(tag -> 1)))
      } yield ()
    }
  }

  test("Tagging a resource fails if it doesn't exist") {
    resources.tag(nxv + "other", projectRef, None, tag, 1, 1).intercept[ResourceNotFound]
  }

  test("Tagging a resource fails if the revision passed is incorrect") {
    givenAResource() { id =>
      val expectedError = IncorrectRev(provided = 3, expected = 1)
      resources.tag(id, projectRef, None, tag, 1, 3).interceptEquals(expectedError)
    }
  }

  test("Tagging a resource succeeds if deprecated") {
    givenADeprecatedResource() { id =>
      resources.tag(id, projectRef, None, tag, 2, 2)
    }
  }

  test("Tagging a resource fails if schemas do not match") {
    givenAResource(Latest(schema1)) { id =>
      resources.tag(id, projectRef, Some(unconstrained), tag, 1, 1).intercept[UnexpectedResourceSchema]
    }
  }

  test("Tagging a resource fails if tag revision not found") {
    givenAResource() { id =>
      resources
        .tag(id, projectRef, Some(unconstrained), tag, 6, 1)
        .interceptEquals(
          RevisionNotFound(provided = 6, current = 1)
        )
    }
  }

  test("Tagging a resource fails if project does not exist") {
    resources.tag(genId(), unknownProject, None, tag, 2, 1).intercept[ProjectNotFound]
  }

  test("Tagging a resource fails if project is deprecated") {
    resources.tag(genId(), projectDeprecated.ref, None, tag, 2, 1).intercept[ProjectIsDeprecated]
  }

  // -----------------------------------------
  // Deprecating a resource
  // -----------------------------------------

  test("Deprecating a resource succeeds") {
    givenAResource(Latest(schema1)) { id =>
      resources
        .deprecate(id, projectRef, Some(schema1), 1)
        .map(r => (r.rev, r.deprecated))
        .assertEquals((2, true))
    }
  }

  test("Deprecating a resource fails if it doesn't exist") {
    resources.deprecate(nxv + "other", projectRef, None, 1).intercept[ResourceNotFound]
  }

  test("Deprecating a resource fails if the revision passed is incorrect") {
    givenAResource() { id =>
      resources.deprecate(id, projectRef, None, 3).interceptEquals(IncorrectRev(3, 1)) >>
        assertActive(id, projectRef)
    }
  }

  test("Deprecating a resource fails if deprecated") {
    givenADeprecatedResource() { id =>
      resources.deprecate(id, projectRef, None, 2).intercept[ResourceIsDeprecated] >>
        assertDeprecated(id, projectRef, None)
    }
  }

  test("Deprecating a resource fails if schemas do not match") {
    givenAResource() { id =>
      resources.deprecate(id, projectRef, Some(schema1), 1).intercept[UnexpectedResourceSchema] >>
        assertActive(id, projectRef)
    }
  }

  test("Deprecating a resource fails if project does not exist") {
    givenAResource() { id =>
      resources.deprecate(id, unknownProject, None, 1).intercept[ProjectNotFound]
    }
  }

  test("Deprecating a resource fails if project is deprecated") {
    resources.deprecate(nxv + "id", projectDeprecated.ref, None, 1).intercept[ProjectIsDeprecated]
  }

  // -----------------------------------------
  // Undeprecating a resource
  // -----------------------------------------

  test("Undeprecating a resource succeeds") {
    givenADeprecatedResource() { id =>
      resources.undeprecate(id, projectRef, None, 2).map(_.deprecated).assertEquals(false) >>
        assertActive(id, projectRef)
    }
  }

  test("Undeprecating a resource fails if resource does not exist") {
    givenADeprecatedResource() { _ =>
      val wrongId = nxv + genString()
      resources.undeprecate(wrongId, projectRef, None, 2).intercept[ResourceNotFound]
    }
  }

  test("Undeprecating a resource fails if resource revision is incorrect") {
    givenADeprecatedResource() { id =>
      resources.undeprecate(id, projectRef, None, 4).intercept[IncorrectRev] >>
        assertDeprecated(id, projectRef, None)
    }
  }

  test("Undeprecating a resource fails if resource is not deprecated") {
    givenAResource() { id =>
      resources.undeprecate(id, projectRef, None, 1).intercept[ResourceIsNotDeprecated] >>
        assertActive(id, projectRef)
    }
  }

  test("Undeprecating a resource fails if schema does not match") {
    givenADeprecatedResource() { id =>
      resources.undeprecate(id, projectRef, Some(schema1), 2).intercept[UnexpectedResourceSchema] >>
        assertDeprecated(id, projectRef, None)
    }
  }

  test("Undeprecating a resource fails if project does not exist") {
    givenADeprecatedResource() { id =>
      val wrongProject = ProjectRef.unsafe(genString(), genString())
      resources.undeprecate(id, wrongProject, None, 2).intercept[ProjectNotFound]
    }
  }

  // -----------------------------------------
  // Fetching a resource
  // -----------------------------------------

  test("Fetching a resource succeeds") {
    givenATaggedResource() { id =>
      (None, Some(IdSegment(unconstrained))).traverse { schema =>
        resources
          .fetch(id, projectRef, schema)
          .map(r => (r.rev, r.value.tags))
          .assertEquals((2, Tags(tag -> 1)), s"for schema $schema")
      }
    }
  }

  test("Fetching a resource succeeds by tag") {
    givenATaggedResource() { id =>
      val segment = nxvSegment(id)
      (None, Some(IdSegment(unconstrained))).traverse { schema =>
        resources
          .fetch(IdSegmentRef(segment, tag), projectRef, schema)
          .map(_.rev)
          .assertEquals(1, s"for schema $schema")
      }
    }
  }

  test("Fetching a resource succeeds by rev") {
    givenAResource() { id =>
      resources.update(id, projectRef, None, 1, sourceWithoutId.deepMerge(json"""{"number": 1}"""), None) >>
        (None, Some(IdSegment(unconstrained))).traverse { schema =>
          resources.fetch(IdSegmentRef(id, 1), projectRef, schema).map(_.rev).assertEquals(1, s"for schema $schema")
        }
    }
  }

  test("Fetching a resource fails if tag does not exist") {
    val otherTag = UserTag.unsafe("other")
    givenAResource() { id =>
      (None, Some(IdSegment(unconstrained))).traverse { schema =>
        resources.fetch(IdSegmentRef(id, otherTag), projectRef, schema).interceptEquals(TagNotFound(otherTag))
      }
    }
  }

  test("Fetching a resource fails if revision does not exist") {
    givenAResource() { id =>
      (None, Some(IdSegment(unconstrained))).traverse { schema =>
        resources
          .fetch(IdSegmentRef(id, 5), projectRef, schema)
          .interceptEquals(
            RevisionNotFound(provided = 5, current = 1)
          )
      }
    }
  }

  test("Fetching a resource fails if resource does not exist") {
    val id = nxv + "notFound"
    for {
      _ <- resources.fetch(id, projectRef, None).intercept[ResourceNotFound]
      _ <- resources.fetch(IdSegmentRef(id, tag), projectRef, None).intercept[ResourceNotFound]
      _ <- resources.fetch(IdSegmentRef(id, 2), projectRef, None).intercept[ResourceNotFound]
    } yield ()
  }

  test("Fetching a resource fails if schema is not resource schema") {
    givenATaggedResource() { id =>
      for {
        _ <- resources.fetch(id, projectRef, Some(schema1)).intercept[ResourceNotFound]
        _ <- resources.fetch(IdSegmentRef(id, tag), projectRef, Some(schema1)).intercept[ResourceNotFound]
        _ <- resources.fetch(IdSegmentRef(id, 2), projectRef, Some(schema1)).intercept[ResourceNotFound]
      } yield ()
    }
  }

  test("Fetching a resource fails if project does not exist") {
    resources.fetch(genId(), unknownProject, None).intercept[ProjectNotFound]
  }

  test("Fetching a resource fails if resource does not exist on deprecated project") {
    resources.fetch(genId(), projectDeprecated.ref, None).intercept[ResourceNotFound]
  }

  // -----------------------------------------
  // Deleting a tag
  // -----------------------------------------

  test("Deleting a tag succeeds") {
    givenATaggedResource() { id =>
      resources.deleteTag(id, projectRef, Some(unconstrained), tag, 2).map { r =>
        assertEquals(r.rev, 3)
        assertEquals(r.value.tags, Tags.empty)
      }
    }
  }

  test("Deleting a tag fails if the resource doesn't exist") {
    resources.deleteTag(nxv + "other", projectRef, None, tag, 1).intercept[ResourceNotFound]
  }

  test("Deleting a tag fails if the revision passed is incorrect") {
    val expectedError = IncorrectRev(provided = 4, expected = 2)
    givenATaggedResource() { id =>
      resources.deleteTag(id, projectRef, None, tag, 4).interceptEquals(expectedError)
    }
  }

  test("Deleting a tag fails if schemas do not match") {
    givenATaggedResource(Latest(schema1)) { id =>
      resources.deleteTag(id, projectRef, Some(unconstrained), tag, 2).intercept[UnexpectedResourceSchema]
    }
  }

  test("Deleting a tag fails if the tag doesn't exist") {
    givenAResource() { id =>
      resources.deleteTag(id, projectRef, Some(unconstrained), tag, 1).intercept[TagNotFound]
    }
  }

  // -----------------------------------------
  // Deleting a resource
  // -----------------------------------------

  test("Deleting a resource succeeds for an existing resource") {
    givenAResource() { id =>
      for {
        _ <- resources.delete(id, projectRef)
        _ <- resources.fetch(id, projectRef, None).intercept[ResourceNotFound]
      } yield ()
    }
  }

  test("Deleting a resource fails if it doesn't exist") {
    resources.delete(nxv + "xxx", projectRef).intercept[ResourceNotFound]
  }

  test("Deleting a resource fails if project is deprecated") {
    resources.delete(nxv + "xxx", projectDeprecated.ref).intercept[ProjectIsDeprecated]
  }

  // -----------------------------------------
  // Helpers
  // -----------------------------------------

  /** Provides a newly created resource for assertion. Latest revision is 1 */
  private def givenAResource[A](schemaRef: ResourceRef = unconstrainedRef)(assertion: Iri => IO[A]): IO[A] = {
    val id = genId()
    resources.create(id, projectRef, schemaRef, simpleSourcePayload(id), None) >> assertion(id)
  }

  /** Provides a tagged resource for assertion. Latest revision is 2 */
  private def givenATaggedResource[A](schemaRef: ResourceRef = unconstrainedRef)(assertion: Iri => IO[A]): IO[A] =
    givenAResource(schemaRef) { id =>
      resources.tag(id, projectRef, Some(schemaRef.iri), tag, 1, 1) >> assertion(id)
    }

  /** Provides a deprecated resource for assertion. Latest revision is 2 */
  private def givenADeprecatedResource[A](schemaRef: ResourceRef = unconstrainedRef)(assertion: Iri => IO[A]): IO[A] =
    givenAResource(schemaRef) { id =>
      resources.deprecate(id, projectRef, Some(schemaRef.iri), 1).map(_.deprecated).assertEquals(true) >> assertion(id)
    }

  private def assertDeprecated(id: IdSegment, projectRef: ProjectRef, schema: Option[IdSegment])(using
      Location
  ): IO[Unit] =
    resources.fetch(id, projectRef, schema).map(_.deprecated).assertEquals(true)

  private def assertActive(id: IdSegment, projectRef: ProjectRef, schema: Option[IdSegment] = None)(using
      Location
  ): IO[Unit] =
    resources.fetch(id, projectRef, schema).map(_.deprecated).assertEquals(false)

}
