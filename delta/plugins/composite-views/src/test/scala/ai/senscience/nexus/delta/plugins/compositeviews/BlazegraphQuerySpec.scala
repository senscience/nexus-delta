package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.elasticsearch.client.ElasticsearchMappings
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryClientDummy
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.SparqlNTriples
import ai.senscience.nexus.delta.plugins.blazegraph.model.permissions
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.{commonNamespace, projectionNamespace}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.{ElasticSearchProjection, SparqlProjection}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.ProjectionNotFound
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.ProjectSource
import ai.senscience.nexus.delta.plugins.compositeviews.model.ProjectionType.SparqlProjectionType
import ai.senscience.nexus.delta.plugins.compositeviews.model.TemplateSparqlConstructQuery
import ai.senscience.nexus.delta.plugins.compositeviews.test.{expandOnlyIris, expectIndexingView}
import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Group, User}
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.data.NonEmptyList
import io.circe.JsonObject
import org.scalatest.CancelAfterFailure

import java.util.UUID

class BlazegraphQuerySpec extends CatsEffectSpec with CancelAfterFailure {

  private val realm         = Label.unsafe("myrealm")
  private val alice: Caller = Caller(User("Alice", realm), Set(User("Alice", realm), Group("users", realm)))
  private given bob: Caller = Caller(User("Bob", realm), Set(User("Bob", realm), Group("users", realm)))
  private val anon: Caller  = Caller(Anonymous, Set(Anonymous))

  private val project   = ProjectGen.project("myorg", "proj")
  private val otherPerm = Permission.unsafe("other")

  private val aclCheck = AclSimpleCheck(
    (alice.subject, AclAddress.Project(project.ref), Set(permissions.query)),
    (bob.subject, AclAddress.Project(project.ref), Set(permissions.query, otherPerm)),
    (anon.subject, AclAddress.Root, Set(permissions.read))
  ).accepted

  private val construct = TemplateSparqlConstructQuery(
    "prefix p: <http://localhost/>\nCONSTRUCT{ {resource_id} p:transformed ?v } WHERE { {resource_id} p:predicate ?v}"
  ).rightValue

  private val id   = iri"http://localhost/${genString()}"
  private val uuid = UUID.randomUUID()

  private def blazeProjection(id: Iri, permission: Permission) =
    SparqlProjection(
      id,
      UUID.randomUUID(),
      IndexingRev.init,
      construct,
      IriFilter.None,
      IriFilter.None,
      false,
      false,
      permission
    )

  private val blazeProjection1 = blazeProjection(nxv + "blaze1", permissions.query)
  private val blazeProjection2 = blazeProjection(nxv + "blaze2", otherPerm)

  private val esProjection =
    ElasticSearchProjection(
      nxv + "es1",
      UUID.randomUUID(),
      IndexingRev.init,
      construct,
      IriFilter.None,
      IriFilter.None,
      false,
      false,
      false,
      permissions.query,
      None,
      ElasticsearchMappings(JsonObject.empty),
      None,
      ContextObject.empty
    )

  private val projectSource =
    ProjectSource(nxv + "source1", UUID.randomUUID(), IriFilter.None, IriFilter.None, None, false)

  private val indexingView = ActiveViewDef(
    ViewRef(project.ref, id),
    uuid,
    1,
    CompositeViewFactory.unsafe(
      NonEmptyList.of(projectSource),
      NonEmptyList.of(blazeProjection1, blazeProjection2, esProjection),
      None
    )
  )

  private val prefix = "prefix"

  // projection namespaces
  private val blazeP1Ns     = projectionNamespace(blazeProjection1, uuid, prefix)
  private val blazeP2Ns     = projectionNamespace(blazeProjection2, uuid, prefix)
  private val blazeCommonNs = commonNamespace(uuid, indexingView.value.sourceIndexingRev, prefix)

  private val responseCommonNs   = NTriples("blazeCommonNs", BNode.random)
  private val responseBlazeP1Ns  = NTriples("blazeP1Ns", BNode.random)
  private val responseBlazeP12Ns = NTriples("blazeP1Ns-blazeP2Ns", BNode.random)

  private val viewsQuery =
    BlazegraphQuery(
      aclCheck,
      expectIndexingView(indexingView),
      expandOnlyIris,
      new SparqlQueryClientDummy(sparqlNTriples = {
        case seq if seq.toSet == Set(blazeCommonNs)        => responseCommonNs
        case seq if seq.toSet == Set(blazeP1Ns)            => responseBlazeP1Ns
        case seq if seq.toSet == Set(blazeP1Ns, blazeP2Ns) => responseBlazeP12Ns
        case _                                             => NTriples.empty
      }),
      prefix
    )

  "A BlazegraphQuery" should {

    "query the common Blazegraph namespace" in {
      viewsQuery.query(id, project.ref, construct, SparqlNTriples).accepted.value shouldEqual responseCommonNs
      viewsQuery.query(id, project.ref, construct, SparqlNTriples)(using alice).rejectedWith[AuthorizationFailed]
      viewsQuery.query(id, project.ref, construct, SparqlNTriples)(using anon).rejectedWith[AuthorizationFailed]
    }

    "query all the Blazegraph projections' namespaces" in {
      forAll(List(alice -> responseBlazeP1Ns, bob -> responseBlazeP12Ns)) { case (caller, expected) =>
        viewsQuery.queryProjections(id, project.ref, construct, SparqlNTriples)(using caller).accepted.value shouldEqual
          expected
      }
      viewsQuery
        .queryProjections(id, project.ref, construct, SparqlNTriples)(using anon)
        .rejectedWith[AuthorizationFailed]
    }

    "query a Blazegraph projections' namespace" in {
      val blaze1 = nxv + "blaze1"
      val es     = nxv + "es1"
      viewsQuery.query(id, blaze1, project.ref, construct, SparqlNTriples)(using bob).accepted.value shouldEqual
        responseBlazeP1Ns
      viewsQuery.query(id, blaze1, project.ref, construct, SparqlNTriples)(using anon).rejectedWith[AuthorizationFailed]
      viewsQuery.query(id, es, project.ref, construct, SparqlNTriples)(using bob).rejected shouldEqual
        ProjectionNotFound(id, es, project.ref, SparqlProjectionType)
    }
  }

}
