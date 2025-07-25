package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeView.Interval
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.{ElasticSearchProjection, SparqlProjection}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjectionFields.{ElasticSearchProjectionFields, SparqlProjectionFields}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.{CrossProjectSource, ProjectSource, RemoteProjectSource}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSourceFields.{CrossProjectSourceFields, ProjectSourceFields, RemoteProjectSourceFields}
import ai.senscience.nexus.delta.plugins.compositeviews.model.{permissions, CompositeViewFields, TemplateSparqlConstructQuery}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.views.IndexingRev
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Identity, IriFilter, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.data.NonEmptyList
import cats.effect.IO
import io.circe.{Json, JsonObject}
import org.http4s.Uri

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

trait CompositeViewsFixture extends ConfigFixtures {

  val alwaysValidate: ValidateCompositeView = (_, _) => IO.unit

  val query =
    TemplateSparqlConstructQuery(
      "prefix p: <http://localhost/>\nCONSTRUCT{ {resource_id} p:transformed ?v } WHERE { {resource_id} p:predicate ?v}"
    ).getOrElse(throw new RuntimeException("Should never happen"))

  val uuid                          = UUID.fromString("f8468909-a797-4b10-8b5f-000cba337bfa")
  implicit val uuidF: UUIDF         = UUIDF.fixed(uuid)
  implicit val ec: ExecutionContext = ExecutionContext.global

  val epoch           = Instant.EPOCH
  val epochPlus10     = Instant.EPOCH.plusMillis(10L)
  val subject         = User("myuser", Label.unsafe("myrealm"))
  val id              = iri"http://localhost/${UUID.randomUUID()}"
  val project         = ProjectGen.project("myorg", "myproj")
  val projectRef      = project.ref
  val otherProjectRef = ProjectGen.project("org", "otherproject").ref
  val source          = Json.obj()
  val source2         = Json.obj("key" -> Json.fromInt(1))

  val projectFields      = ProjectSourceFields(
    Some(iri"http://example.com/project-source"),
    IriFilter.None,
    IriFilter.None,
    None
  )
  val crossProjectFields = CrossProjectSourceFields(
    Some(iri"http://example.com/cross-project-source"),
    ProjectRef(Label.unsafe("org"), Label.unsafe("otherproject")),
    identities = Set(Identity.Anonymous)
  )

  val remoteProjectFields = RemoteProjectSourceFields(
    Some(iri"http://example.com/remote-project-source"),
    ProjectRef(Label.unsafe("org"), Label.unsafe("remoteproject")),
    Uri.unsafeFromString("http://example.com/remote-endpoint")
  )

  val esProjectionFields         = ElasticSearchProjectionFields(
    Some(iri"http://example.com/es-projection"),
    query,
    None,
    JsonObject(),
    ContextObject(JsonObject.empty),
    Some(JsonObject())
  )
  val blazegraphProjectionFields = SparqlProjectionFields(
    Some(iri"http://example.com/blazegraph-projection"),
    query
  )

  val viewFields = CompositeViewFields(
    NonEmptyList.of(projectFields, crossProjectFields, remoteProjectFields),
    NonEmptyList.of(esProjectionFields, blazegraphProjectionFields),
    Some(Interval(1.minute))
  )

  val updatedFields = viewFields.copy(rebuildStrategy = Some(Interval(2.minutes)))

  val projectSource = ProjectSource(
    iri"http://example.com/project-source",
    uuid,
    IriFilter.None,
    IriFilter.None,
    None,
    false
  )

  val otherProject = ProjectRef(Label.unsafe("org"), Label.unsafe("otherproject"))

  val crossProjectSource = CrossProjectSource(
    iri"http://example.com/cross-project-source",
    uuid,
    IriFilter.None,
    IriFilter.None,
    None,
    false,
    otherProject,
    identities = Set(Identity.Anonymous)
  )

  val remoteProjectSource = RemoteProjectSource(
    iri"http://example.com/remote-project-source",
    uuid,
    IriFilter.None,
    IriFilter.None,
    None,
    false,
    ProjectRef(Label.unsafe("org"), Label.unsafe("remoteproject")),
    Uri.unsafeFromString("http://example.com/remote-endpoint")
  )

  val esProjection         = ElasticSearchProjection(
    iri"http://example.com/es-projection",
    uuid,
    IndexingRev.init,
    query,
    IriFilter.None,
    IriFilter.None,
    false,
    false,
    false,
    permissions.query,
    None,
    JsonObject(),
    Some(JsonObject()),
    ContextObject(JsonObject.empty)
  )
  val blazegraphProjection = SparqlProjection(
    iri"http://example.com/blazegraph-projection",
    uuid,
    IndexingRev.init,
    query,
    IriFilter.None,
    IriFilter.None,
    false,
    false,
    permissions.query
  )

  val viewValue      = CompositeViewFactory.unsafe(
    NonEmptyList.of(projectSource, crossProjectSource, remoteProjectSource),
    NonEmptyList.of(esProjection, blazegraphProjection),
    Some(Interval(1.minute))
  )
  val viewValueNamed = viewValue.copy(name = Some("viewName"), description = Some("viewDescription"))
  val updatedValue   = viewValue.copy(rebuildStrategy = Some(Interval(2.minutes)))

  val batchConfig = BatchConfig(10, 10.seconds)
}

object CompositeViewsFixture extends CompositeViewsFixture
