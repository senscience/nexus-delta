package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.*
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjectionFields.{ElasticSearchProjectionFields, SparqlProjectionFields}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.*
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSourceFields.*
import ai.senscience.nexus.delta.plugins.compositeviews.model.permissions
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sdk.projects.model.ProjectBase
import ai.senscience.nexus.delta.sdk.views.IndexingRev
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Identity, IriFilter, Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.{Json, JsonObject}
import org.http4s.Uri

import java.util.UUID

class CompositeViewFactorySuite extends NexusSuite {

  implicit private val projectBase: ProjectBase = ProjectBase(iri"http://localhost/project")
  private val uuid                              = UUID.randomUUID()
  implicit private val uuidF: UUIDF             = UUIDF.fixed(uuid)

  private val schemas: IriFilter = IriFilter.restrictedTo(nxv + "Schema")
  private val types: IriFilter   = IriFilter.restrictedTo(nxv + "Type")
  private val tag: Some[UserTag] = Some(UserTag.unsafe("tag"))
  private val includeDeprecated  = true
  private val includeMetadata    = true
  private val includeContext     = true

  private val projectSourceId     = iri"http://localhost/project-source"
  private val projectSourceFields = ProjectSourceFields(
    Some(projectSourceId),
    schemas,
    types,
    tag,
    includeDeprecated
  )

  private val crossSourceId     = iri"http://localhost/cross-project-source"
  private val crossSourceFields = CrossProjectSourceFields(
    Some(crossSourceId),
    ProjectRef(Label.unsafe("org"), Label.unsafe("otherproject")),
    identities = Set(Identity.Anonymous),
    schemas,
    types,
    tag,
    includeDeprecated
  )

  private val remoteSourceId     = iri"http://localhost/remote-project-source"
  private val remoteSourceFields = RemoteProjectSourceFields(
    Some(remoteSourceId),
    ProjectRef.unsafe("org", "remoteproject"),
    Uri.unsafeFromString("http://example.com/remote-endpoint"),
    schemas,
    types,
    tag,
    includeDeprecated
  )

  private val esIndexingDef      = ElasticsearchIndexDef.empty
  private val esProjectionId     = iri"http://localhost/es-projection"
  private val esProjectionFields = ElasticSearchProjectionFields(
    Some(esProjectionId),
    SparqlConstructQuery.unsafe("CONSTRUCT..."),
    None,
    esIndexingDef.mappings,
    ContextObject(JsonObject("context" -> Json.obj())),
    esIndexingDef.settings,
    schemas,
    types,
    includeDeprecated,
    includeMetadata,
    includeContext
  )

  private val blazegraphProjectionId     = iri"http://example.com/blazegraph-projection"
  private val blazegraphProjectionFields = SparqlProjectionFields(
    Some(blazegraphProjectionId),
    SparqlConstructQuery.unsafe("CONSTRUCT..."),
    schemas,
    types,
    includeDeprecated,
    includeMetadata
  )

  test("Create the matching source from the project source field with a defined id") {
    CompositeViewFactory
      .create(projectSourceFields)
      .assertEquals(
        projectSourceId -> ProjectSource(projectSourceId, uuid, schemas, types, tag, includeDeprecated)
      )
  }

  test("Create the matching source from the project source field with no id") {
    val expectedId = projectBase.iri / uuid.toString
    CompositeViewFactory
      .create(projectSourceFields.copy(id = None))
      .assertEquals(
        expectedId -> ProjectSource(expectedId, uuid, schemas, types, tag, includeDeprecated)
      )
  }

  test("Create the matching source from the cross project source field with a defined id") {
    CompositeViewFactory
      .create(crossSourceFields)
      .assertEquals(
        crossSourceId -> CrossProjectSource(
          crossSourceId,
          uuid,
          schemas,
          types,
          tag,
          includeDeprecated,
          crossSourceFields.project,
          crossSourceFields.identities
        )
      )
  }

  test("Create the matching source from the remote project source field with a defined id") {
    CompositeViewFactory
      .create(remoteSourceFields)
      .assertEquals(
        remoteSourceId -> RemoteProjectSource(
          remoteSourceId,
          uuid,
          schemas,
          types,
          tag,
          includeDeprecated,
          remoteSourceFields.project,
          remoteSourceFields.endpoint
        )
      )
  }

  test("Create the matching projection from the Elasticsearch projection field with a defined id") {
    val nextRev = IndexingRev(5)
    CompositeViewFactory
      .create(esProjectionFields, nextRev)
      .assertEquals(
        esProjectionId -> ElasticSearchProjection(
          esProjectionId,
          uuid,
          indexingRev = nextRev,
          esProjectionFields.query,
          schemas,
          types,
          includeMetadata,
          includeDeprecated,
          includeContext,
          permissions.query,
          None,
          esProjectionFields.mapping,
          esProjectionFields.settings,
          esProjectionFields.context
        )
      )
  }

  test("Create the matching projection from the Blazegraph projection field with a defined id") {
    val nextRev = IndexingRev(5)
    CompositeViewFactory
      .create(blazegraphProjectionFields, nextRev)
      .assertEquals(
        blazegraphProjectionId -> SparqlProjection(
          blazegraphProjectionId,
          uuid,
          indexingRev = nextRev,
          blazegraphProjectionFields.query,
          schemas,
          types,
          includeMetadata,
          includeDeprecated,
          permissions.query
        )
      )
  }

  test("Create a source when upserting a non-existing source") {
    CompositeViewFactory
      .upsert(projectSourceFields, _ => None)
      .assertEquals(
        projectSourceId -> ProjectSource(projectSourceId, uuid, schemas, types, tag, includeDeprecated)
      )
  }

  test("Preserve the uuid when upserting an existing source") {
    val current = ProjectSource(projectSourceId, UUID.randomUUID(), schemas, types, tag, includeDeprecated)
    CompositeViewFactory
      .upsert(projectSourceFields, _ => Some(current))
      .map(_._2.uuid)
      .assertEquals(current.uuid)
  }

  test("Create a projection when upserting a non-existing projection") {
    val nextRev = IndexingRev(5)
    CompositeViewFactory
      .upsert(blazegraphProjectionFields, _ => None, nextRev, false)
      .assertEquals(
        blazegraphProjectionId -> SparqlProjection(
          blazegraphProjectionId,
          uuid,
          indexingRev = nextRev,
          blazegraphProjectionFields.query,
          schemas,
          types,
          includeMetadata,
          includeDeprecated,
          permissions.query
        )
      )
  }

  test("Preserve the uuid and the indexing rev when sources and projection have not changed.") {
    val nextRev       = IndexingRev(5)
    val projectionRev = IndexingRev(3)
    val current       = SparqlProjection(
      blazegraphProjectionId,
      uuid,
      indexingRev = projectionRev,
      blazegraphProjectionFields.query,
      schemas,
      types,
      includeMetadata,
      includeDeprecated,
      permissions.query
    )
    CompositeViewFactory
      .upsert(blazegraphProjectionFields, _ => Some(current), nextRev, false)
      .map { case (_, p) =>
        p.uuid -> p.indexingRev
      }
      .assertEquals(current.uuid -> projectionRev)
  }

  test("Preserve the uuid and update the indexing rev when source has changed.") {
    val nextRev       = IndexingRev(5)
    val projectionRev = IndexingRev(3)
    val current       = SparqlProjection(
      blazegraphProjectionId,
      uuid,
      indexingRev = projectionRev,
      blazegraphProjectionFields.query,
      schemas,
      types,
      includeMetadata,
      includeDeprecated,
      permissions.query
    )
    CompositeViewFactory
      .upsert(blazegraphProjectionFields, _ => Some(current), nextRev, true)
      .map { case (_, p) =>
        p.uuid -> p.indexingRev
      }
      .assertEquals(current.uuid -> nextRev)
  }

  test("Preserve the uuid and update the indexing rev when the projection has changed.") {
    val nextRev       = IndexingRev(5)
    val projectionRev = IndexingRev(3)
    val current       = SparqlProjection(
      blazegraphProjectionId,
      uuid,
      indexingRev = projectionRev,
      blazegraphProjectionFields.query,
      schemas,
      IriFilter.restrictedTo(nxv + "OldType"),
      includeMetadata,
      includeDeprecated,
      permissions.query
    )
    CompositeViewFactory
      .upsert(blazegraphProjectionFields, _ => Some(current), nextRev, false)
      .map { case (_, p) =>
        p.uuid -> p.indexingRev
      }
      .assertEquals(current.uuid -> nextRev)
  }
}
