package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.plugins.compositeviews.model.*
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectRejection}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sourcing.model.Identity.{Group, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ai.senscience.nexus.testkit.CirceEq
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import io.circe.Json
import org.scalatest.Assertion
import org.scalatest.matchers.{BeMatcher, MatchResult}

import java.time.Instant
import scala.concurrent.duration.*

class CompositeViewsSpec
    extends CatsEffectSpec
    with DoobieScalaTestFixture
    with CompositeViewsFixture
    with CirceEq
    with Fixtures {
  private val realm                  = Label.unsafe("myrealm")
  implicit private val alice: Caller = Caller(User("Alice", realm), Set(User("Alice", realm), Group("users", realm)))

  implicit private val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  "CompositeViews" should {
    val apiMappings       = ApiMappings("nxv" -> nxv.base)
    val base              = nxv.base
    val project           = ProjectGen.project("org", "proj", base = base, mappings = apiMappings)
    val deprecatedProject = ProjectGen.project("org", "proj-deprecated")
    val listProject       = ProjectGen.project("org", "list", base = base, mappings = apiMappings)

    val projectRef = project.ref

    val fetchContext = FetchContextDummy(
      Map(project.ref -> project.context, listProject.ref -> listProject.context),
      Set(deprecatedProject.ref)
    )

    lazy val compositeViews = CompositeViews(
      fetchContext,
      ResolverContextResolution(rcr),
      alwaysValidate,
      1.minute,
      eventLogConfig,
      xas,
      clock
    ).accepted

    val viewSource        = jsonContentOf("composite-view-source.json")
    val viewSourceUpdated = jsonContentOf("composite-view-source-updated.json")

    val viewId      = project.base.iri / uuid.toString
    val otherViewId = iri"http://example.com/other-view"
    def resourceFor(
        id: Iri,
        value: CompositeViewValue,
        rev: Int = 1,
        deprecated: Boolean = false,
        createdAt: Instant = Instant.EPOCH,
        createdBy: Subject = alice.subject,
        updatedAt: Instant = Instant.EPOCH,
        updatedBy: Subject = alice.subject,
        source: Json
    ): ViewResource = CompositeViewsGen.resourceFor(
      projectRef,
      id,
      uuid,
      value,
      rev = rev,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      source = source
    )

    "create a composite view" when {
      "using JSON source" in {
        compositeViews.create(projectRef, viewSource).accepted shouldEqual resourceFor(
          viewId,
          viewValue,
          source = viewSource.removeAllKeys("token")
        )
      }

      "using CompositeViewFields" in {
        val result   = compositeViews.create(otherViewId, projectRef, viewFields).accepted
        val expected = resourceFor(otherViewId, viewValue, source = viewSource)

        assertEqualViews(result, expected)
      }

    }

    "reject creating a view" when {
      "view already exists" in {
        compositeViews.create(projectRef, viewSource).rejectedWith[ViewAlreadyExists]
      }
    }

    "update a view" when {
      "using JSON source" in {
        compositeViews.update(viewId, projectRef, 1, viewSourceUpdated).accepted shouldEqual resourceFor(
          viewId,
          updatedValue,
          source = viewSourceUpdated.removeAllKeys("token"),
          rev = 2
        )
      }

      "using CompositeViewFields" in {
        val result   = compositeViews.update(otherViewId, projectRef, 1, updatedFields).accepted
        val expected = resourceFor(otherViewId, updatedValue, source = viewSourceUpdated, rev = 2)

        assertEqualViews(result, expected)
      }
    }

    "reject updating a view" when {
      "rev provided is wrong " in {
        compositeViews.update(viewId, projectRef, 1, viewSourceUpdated).rejectedWith[IncorrectRev]
      }
    }

    "deprecate a view" in {
      val deprecated = compositeViews.deprecate(otherViewId, projectRef, 2).accepted
      val expected   = resourceFor(otherViewId, updatedValue, source = viewSourceUpdated, rev = 3, deprecated = true)

      assertEqualViews(deprecated, expected)
      compositeViews.fetch(otherViewId, projectRef).accepted should be(deprecated)
    }

    "reject deprecating a view" when {
      "views is already deprecated" in {
        compositeViews.deprecate(otherViewId, projectRef, 3).rejectedWith[ViewIsDeprecated]
      }
      "incorrect revision is provided" in {
        compositeViews.deprecate(otherViewId, projectRef, 2).rejectedWith[IncorrectRev]
      }
    }

    "undeprecate a view" in {
      givenADeprecatedView { view =>
        compositeViews.undeprecate(view, projectRef, 2).accepted should not be deprecated
        compositeViews.fetch(view, projectRef).accepted should not be deprecated
      }
    }

    "reject undeprecating a view" when {
      "view is not deprecated" in {
        givenAView { view =>
          compositeViews.undeprecate(view, projectRef, 1).assertRejectedWith[ViewIsNotDeprecated]
        }
      }
      "incorrect revision is provided" in {
        givenADeprecatedView { view =>
          compositeViews.undeprecate(view, projectRef, 1).assertRejectedWith[IncorrectRev]
        }
      }
      "project does not exist" in {
        val nonexistentProject = ProjectRef.unsafe("org", genString())
        givenADeprecatedView { view =>
          compositeViews.undeprecate(view, nonexistentProject, 1).assertRejectedWith[ProjectRejection.ProjectNotFound]
        }
      }
    }

    "fetch a view" when {
      "no tag or rev is provided" in {
        compositeViews.fetch(viewId, projectRef).accepted shouldEqual resourceFor(
          viewId,
          updatedValue,
          source = viewSourceUpdated.removeAllKeys("token"),
          rev = 2
        )
      }
      "rev is provided" in {
        compositeViews.fetch(IdSegmentRef(viewId, 1), projectRef).accepted shouldEqual resourceFor(
          viewId,
          viewValue,
          source = viewSource.removeAllKeys("token")
        )
      }
    }

    "reject fetching a view" when {
      "view doesn't exist" in {
        compositeViews.fetch(iri"http://example.com/wrong", projectRef).rejectedWith[ViewNotFound]
      }
      "revision doesn't exist" in {
        compositeViews.fetch(IdSegmentRef(viewId, 42), projectRef).rejectedWith[RevisionNotFound]
      }

      "attempting any lookup by tag" in {
        val tag = UserTag.unsafe("mytag")
        compositeViews.fetch(IdSegmentRef(viewId, tag), projectRef).rejectedWith[FetchByTagNotSupported]
      }
    }

    def givenAView(test: String => Assertion): Assertion = {
      val viewId = genString()
      compositeViews.create(viewId, projectRef, viewFields).accepted
      test(viewId)
    }

    def givenADeprecatedView(test: String => Assertion): Assertion = {
      givenAView { view =>
        compositeViews.deprecate(view, projectRef, 1).accepted
        compositeViews.fetch(view, projectRef).accepted should be(deprecated)
        test(view)
      }
    }

    def assertEqualViews(cv1: ViewResource, cv2: ViewResource): Assertion = {
      val cvNoSource: ViewResource => ViewResource = cv => cv.copy(value = cv.value.copy(source = Json.obj()))
      cvNoSource(cv1) shouldEqual cvNoSource(cv2)
      cv1.value.source.removeKeys("@id") should equalIgnoreArrayOrder(cv2.value.source.removeKeys("@id"))
    }

    def deprecated: BeMatcher[ViewResource] = BeMatcher { view =>
      MatchResult(
        view.deprecated,
        s"view was not deprecated",
        s"view was deprecated"
      )
    }
  }
}
