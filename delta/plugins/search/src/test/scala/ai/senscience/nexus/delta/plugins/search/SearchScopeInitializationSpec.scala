package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.ViewNotFound
import ai.senscience.nexus.delta.plugins.compositeviews.{CompositeViews, CompositeViewsFixture, Fixtures}
import ai.senscience.nexus.delta.plugins.search.model.SearchConfig.IndexingConfig
import ai.senscience.nexus.delta.plugins.search.model.defaultViewId
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.Defaults
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label}
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ch.epfl.bluebrain.nexus.testkit.scalatest.ce.CatsEffectSpec
import io.circe.JsonObject

import scala.concurrent.duration.*

class SearchScopeInitializationSpec
    extends CatsEffectSpec
    with DoobieScalaTestFixture
    with CompositeViewsFixture
    with Fixtures {

  implicit val baseUri: BaseUri = BaseUri.withoutPrefix(uri"http://localhost")

  private val saRealm: Label              = Label.unsafe("service-accounts")
  private val usersRealm: Label           = Label.unsafe("users")
  implicit private val sa: ServiceAccount = ServiceAccount(User("nexus-sa", saRealm))
  implicit private val bob: Subject       = User("bob", usersRealm)

  private val fetchContext               = FetchContextDummy(List(project))
  private lazy val views: CompositeViews = CompositeViews(
    fetchContext,
    ResolverContextResolution(rcr),
    alwaysValidate,
    1.minute,
    eventLogConfig,
    xas,
    clock
  ).accepted

  private val indexingConfig =
    IndexingConfig(
      IriFilter.None,
      JsonObject.empty,
      None,
      SparqlConstructQuery.unsafe(""),
      ContextObject(JsonObject.empty),
      None
    )

  private val defaults = Defaults("viewName", "viewDescription")
  lazy val scopeInit   = new SearchScopeInitialization(views, indexingConfig, sa, defaults)

  "An SearchScopeInitialization" should {

    "create a composite view on a new project" in {
      views.fetch(defaultViewId, project.ref).rejectedWith[ViewNotFound]
      scopeInit.onProjectCreation(project.ref, bob).accepted
      val view = views.fetch(defaultViewId, project.ref).accepted
      view.rev shouldEqual 1L
      view.createdBy shouldEqual sa.caller.subject
    }

    "not create a composite view if one exists" in {
      views.fetch(defaultViewId, project.ref).accepted.rev shouldEqual 1L
      scopeInit.onProjectCreation(project.ref, bob).accepted
      views.fetch(defaultViewId, project.ref).accepted.rev shouldEqual 1L
    }

  }

}
