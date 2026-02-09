package ai.senscience.nexus.delta.sdk.model.search

import ai.senscience.nexus.delta.sdk.generators.{OrganizationGen, ProjectGen, RealmGen, WellKnownGen}
import ai.senscience.nexus.delta.sdk.model.Name
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.{OrganizationSearchParams, ProjectSearchParams, RealmSearchParams}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, User}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*
import cats.effect.IO

class SearchParamsSuite extends NexusSuite {

  private val subject = User("myuser", Label.unsafe("myrealm"))

  {
    val issuer              = "myrealm"
    val (wellKnownUri, wk)  = WellKnownGen.create(issuer)
    val resource            = RealmGen.resourceFor(RealmGen.realm(wellKnownUri, wk, None), 1, subject)
    val searchWithAllParams = RealmSearchParams(
      issuer = Some(issuer),
      deprecated = Some(false),
      rev = Some(1),
      createdBy = Some(subject),
      updatedBy = Some(subject),
      r => IO.pure(r.name == resource.value.name)
    )

    test("RealmSearchParams should match a realm resource") {
      List(searchWithAllParams, RealmSearchParams(), RealmSearchParams(issuer = Some(issuer))).traverse { search =>
        search.matches(resource).assert
      }
    }

    test("RealmSearchParams should not match a realm resource") {
      List(
        resource.copy(deprecated = true),
        resource.copy(rev = 2),
        resource.map(_.copy(issuer = "other")),
        resource.map(_.copy(name = Name.unsafe("other")))
      ).traverse { resource =>
        searchWithAllParams.matches(resource).assertEquals(false)
      }
    }
  }

  {
    val searchWithAllParams = OrganizationSearchParams(
      deprecated = Some(false),
      rev = Some(1),
      createdBy = Some(subject),
      updatedBy = Some(subject),
      label = Some("myorg"),
      _ => IO.pure(true)
    )
    val resource            = OrganizationGen.resourceFor(OrganizationGen.organization("myorg"), 1, subject)

    test("OrganizationSearchParams should match an organization resource") {
      List(
        searchWithAllParams,
        OrganizationSearchParams(label = Some("my"), filter = _ => IO.pure(true)),
        OrganizationSearchParams(filter = _ => IO.pure(true)),
        OrganizationSearchParams(rev = Some(1), filter = _ => IO.pure(true))
      ).traverse { search =>
        search.matches(resource).assert
      }
    }

    test("OrganizationSearchParams should not match an organization resource") {
      List(
        resource.map(_.copy(label = Label.unsafe("other"))),
        resource.copy(deprecated = true),
        resource.copy(createdBy = Anonymous)
      ).traverse { resource =>
        searchWithAllParams.matches(resource).assertEquals(false)
      }
    }
  }

  {
    val org                 = Label.unsafe("myorg")
    val searchWithAllParams = ProjectSearchParams(
      organization = Some(org),
      deprecated = Some(false),
      rev = Some(1),
      createdBy = Some(subject),
      updatedBy = Some(subject),
      label = Some("myproj"),
      _ => IO.pure(true)
    )
    val resource            = ProjectGen.resourceFor(ProjectGen.project("myorg", "myproj"), 1, subject)

    test("ProjectSearchParams should match a project resource") {
      List(
        searchWithAllParams,
        ProjectSearchParams(label = Some("my"), filter = _ => IO.pure(true)),
        ProjectSearchParams(filter = _ => IO.pure(true)),
        ProjectSearchParams(rev = Some(1), filter = _ => IO.pure(true))
      ).foreach { search =>
        assertEquals(search.matches(resource).accepted, true)
      }
    }

    test("ProjectSearchParams should not match a project resource") {
      List(
        resource.copy(deprecated = true),
        resource.map(_.copy(label = Label.unsafe("o"))),
        resource.map(_.copy(organizationLabel = Label.unsafe("o")))
      ).foreach { resource =>
        assertEquals(searchWithAllParams.matches(resource).accepted, false)
      }
    }
  }

}
