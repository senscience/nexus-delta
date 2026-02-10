package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure
import ai.senscience.nexus.delta.sdk.resolvers.model.IdentityResolution.{ProvidedIdentities, UseCurrentCaller}
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.{CrossProjectValue, InProjectValue}
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Identity.{Authenticated, Group, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import cats.syntax.all.*

class ResolverValueSuite extends NexusSuite with Fixtures {

  private val realm       = Label.unsafe("myrealm")
  private val name        = Some("resolverName")
  private val description = Some("resolverDescription")

  test("InProject should be successfully decoded") {
    ExpandedJsonLd(jsonContentOf("resolvers/expanded/in-project-resolver.json")).map { expanded =>
      expanded.to[ResolverValue].assertRight(InProjectValue(Priority.unsafe(42)))
    }
  }

  test("InProject should be successfully decoded when a name and description are defined") {
    ExpandedJsonLd(jsonContentOf("resolvers/expanded/in-project-resolver-name-desc.json")).map { expanded =>
      expanded.to[ResolverValue].assertRight(InProjectValue(name, description, Priority.unsafe(42)))
    }
  }

  test("InProject should generate the correct source from value") {
    val inProject = InProjectValue(name, description, Priority.unsafe(42))
    assertEquals(
      ResolverValue.generateSource(nxv + "generated", inProject),
      jsonContentOf("resolvers/in-project-from-value.json")
    )
  }

  test("CrossProject should be successfully decoded when using provided entities resolution") {
    List(
      jsonContentOf("resolvers/expanded/cross-project-resolver-identities.json"),
      jsonContentOf("resolvers/expanded/cross-project-resolver-identities-no-type.json")
    ).traverse { json =>
      val expected = CrossProjectValue(
        Priority.unsafe(42),
        Set(nxv.Schema),
        NonEmptyList.of(ProjectRef.unsafe("org", "proj"), ProjectRef.unsafe("org", "proj2")),
        ProvidedIdentities(Set(User("Bob", realm), Group("mygroup", realm), Authenticated(realm)))
      )
      ExpandedJsonLd(json).map { expanded =>
        expanded.to[ResolverValue].assertRight(expected)
      }
    }
  }

  test("CrossProject should be successfully decoded when using current caller resolution") {
    ExpandedJsonLd(jsonContentOf("resolvers/expanded/cross-project-resolver-use-caller.json")).map { expanded =>
      val expected = CrossProjectValue(
        Priority.unsafe(42),
        Set(nxv.Schema),
        NonEmptyList.of(ProjectRef.unsafe("org", "proj"), ProjectRef.unsafe("org", "proj2")),
        UseCurrentCaller
      )
      expanded.to[ResolverValue].assertRight(expected)
    }
  }

  test("CrossProject should be successfully decoded when using current caller resolution with name and description") {
    ExpandedJsonLd(jsonContentOf("resolvers/expanded/cross-project-resolver-use-caller-name-desc.json")).map {
      expanded =>
        val expected = CrossProjectValue(
          name,
          description,
          Priority.unsafe(42),
          Set(nxv.Schema),
          NonEmptyList.of(ProjectRef.unsafe("org", "proj"), ProjectRef.unsafe("org", "proj2")),
          UseCurrentCaller
        )
        expanded.to[ResolverValue].assertRight(expected)
    }
  }

  test("CrossProject should result in an error when both resolutions are defined") {
    ExpandedJsonLd(jsonContentOf("resolvers/expanded/cross-project-resolver-both-error.json")).map { expanded =>
      val expectedError = ParsingFailure("Only 'useCurrentCaller' or 'identities' should be defined")
      expanded.to[ResolverValue].assertLeft(expectedError)
    }
  }

  test("CrossProject should generate the correct source from resolver using provided entities resolution") {
    val crossProjectProject = CrossProjectValue(
      Priority.unsafe(42),
      Set(nxv.Schema),
      NonEmptyList.of(ProjectRef.unsafe("org", "project1"), ProjectRef.unsafe("org", "project2")),
      ProvidedIdentities(Set(User("alice", realm)))
    )
    assertEquals(
      ResolverValue.generateSource(nxv + "generated", crossProjectProject),
      jsonContentOf("resolvers/cross-project-provided-entities-from-value.json")
    )
  }

  test("CrossProject should generate the correct source from resolver using current caller resolution") {
    val crossProjectProject = CrossProjectValue(
      name,
      description,
      Priority.unsafe(42),
      Set(nxv.Schema),
      NonEmptyList.of(ProjectRef.unsafe("org", "project1"), ProjectRef.unsafe("org", "project2")),
      UseCurrentCaller
    )
    assertEquals(
      ResolverValue.generateSource(nxv + "generated", crossProjectProject),
      jsonContentOf("resolvers/cross-project-current-caller-from-value.json")
    )
  }

}
