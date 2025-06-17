package ai.senscience.nexus.delta.plugins.elasticsearch.routes

import ai.senscience.nexus.delta.plugins.elasticsearch.Fixtures
import ai.senscience.nexus.delta.plugins.elasticsearch.model.schema as elasticSearchSchema
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sdk.{ConfigFixtures, ProjectResource}
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler}
import ch.epfl.bluebrain.nexus.akka.marshalling.CirceMarshalling
import ch.epfl.bluebrain.nexus.delta.rdf.utils.JsonKeyOrdering
import ch.epfl.bluebrain.nexus.testkit.*
import ch.epfl.bluebrain.nexus.testkit.scalatest.ce.{CatsEffectSpec, CatsIOValues}
import org.scalatest.CancelAfterFailure

import java.util.UUID

class ElasticSearchViewsRoutesFixtures
    extends CatsEffectSpec
    with RouteHelpers
    with CatsIOValues
    with DoobieScalaTestFixture
    with CirceLiteral
    with CirceEq
    with CancelAfterFailure
    with ConfigFixtures
    with CirceMarshalling
    with Fixtures {

  val uuid: UUID = UUID.randomUUID()

  implicit val ordering: JsonKeyOrdering =
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )

  implicit val baseUri: BaseUri                   = BaseUri.unsafe("http://localhost", "v1")
  implicit val paginationConfig: PaginationConfig = PaginationConfig(5, 10, 5)
  implicit val rejectionHandler: RejectionHandler = RdfRejectionHandler.apply
  implicit val exceptionHandler: ExceptionHandler = RdfExceptionHandler.apply

  val aclCheck: AclSimpleCheck = AclSimpleCheck().accepted

  val realm: Label = Label.unsafe("wonderland")

  val reader = User("reader", realm)
  val writer = User("writer", realm)

  val identities = IdentitiesDummy.fromUsers(reader, writer)

  val project: ProjectResource = ProjectGen.resourceFor(
    ProjectGen.project(
      "myorg",
      "myproject",
      uuid = uuid,
      orgUuid = uuid,
      mappings = ApiMappings("view" -> elasticSearchSchema.iri)
    )
  )
  val projectRef: ProjectRef   = project.value.ref
}
