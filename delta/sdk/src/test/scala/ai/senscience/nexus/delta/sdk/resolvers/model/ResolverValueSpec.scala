package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure
import ai.senscience.nexus.delta.sdk.resolvers.model.IdentityResolution.{ProvidedIdentities, UseCurrentCaller}
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.{CrossProjectValue, InProjectValue}
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Identity.{Authenticated, Group, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.data.NonEmptyList

class ResolverValueSpec extends CatsEffectSpec with Fixtures {

  private val realm       = Label.unsafe("myrealm")
  private val name        = Some("resolverName")
  private val description = Some("resolverDescription")

  "InProject" should {

    "be successfully decoded" in {
      val json     = jsonContentOf("resolvers/expanded/in-project-resolver.json")
      val expanded = ExpandedJsonLd(json).accepted

      expanded.to[ResolverValue].rightValue shouldEqual InProjectValue(
        Priority.unsafe(42)
      )
    }

    "be successfully decoded when a name and description are defined" in {
      val json     = jsonContentOf("resolvers/expanded/in-project-resolver-name-desc.json")
      val expanded = ExpandedJsonLd(json).accepted

      expanded.to[ResolverValue].rightValue shouldEqual InProjectValue(
        name,
        description,
        Priority.unsafe(42)
      )
    }

    "generate the correct source from value" in {
      val inProject = InProjectValue(name, description, Priority.unsafe(42))
      ResolverValue.generateSource(nxv + "generated", inProject) shouldEqual
        jsonContentOf("resolvers/in-project-from-value.json")
    }
  }

  "CrossProject" should {
    "be successfully decoded when using provided entities resolution" in {
      forAll(
        List(
          jsonContentOf("resolvers/expanded/cross-project-resolver-identities.json"),
          jsonContentOf("resolvers/expanded/cross-project-resolver-identities-no-type.json")
        )
      ) { json =>
        val expanded = ExpandedJsonLd(json).accepted
        expanded.to[ResolverValue].rightValue shouldEqual CrossProjectValue(
          Priority.unsafe(42),
          Set(nxv.Schema),
          NonEmptyList.of(ProjectRef.unsafe("org", "proj"), ProjectRef.unsafe("org", "proj2")),
          ProvidedIdentities(Set(User("Bob", realm), Group("mygroup", realm), Authenticated(realm)))
        )
      }
    }

    "be successfully decoded when using current caller resolution" in {
      val json     = jsonContentOf("resolvers/expanded/cross-project-resolver-use-caller.json")
      val expanded = ExpandedJsonLd(json).accepted
      expanded.to[ResolverValue].rightValue shouldEqual CrossProjectValue(
        Priority.unsafe(42),
        Set(nxv.Schema),
        NonEmptyList.of(ProjectRef.unsafe("org", "proj"), ProjectRef.unsafe("org", "proj2")),
        UseCurrentCaller
      )
    }

    "be successfully decoded when using current caller resolution with name and description" in {
      val json     = jsonContentOf("resolvers/expanded/cross-project-resolver-use-caller-name-desc.json")
      val expanded = ExpandedJsonLd(json).accepted
      expanded.to[ResolverValue].rightValue shouldEqual CrossProjectValue(
        name,
        description,
        Priority.unsafe(42),
        Set(nxv.Schema),
        NonEmptyList.of(ProjectRef.unsafe("org", "proj"), ProjectRef.unsafe("org", "proj2")),
        UseCurrentCaller
      )
    }

    "result in an error when both resolutions are defined" in {
      val json     = jsonContentOf("resolvers/expanded/cross-project-resolver-both-error.json")
      val expanded = ExpandedJsonLd(json).accepted
      expanded.to[ResolverValue].leftValue shouldEqual ParsingFailure(
        "Only 'useCurrentCaller' or 'identities' should be defined"
      )
    }

    "generate the correct source from resolver using provided entities resolution" in {
      val crossProjectProject = CrossProjectValue(
        Priority.unsafe(42),
        Set(nxv.Schema),
        NonEmptyList.of(
          ProjectRef.unsafe("org", "project1"),
          ProjectRef.unsafe("org", "project2")
        ),
        ProvidedIdentities(Set(User("alice", realm)))
      )
      ResolverValue.generateSource(nxv + "generated", crossProjectProject) shouldEqual
        jsonContentOf("resolvers/cross-project-provided-entities-from-value.json")
    }

    "generate the correct source from resolver using current caller resolution" in {
      val crossProjectProject = CrossProjectValue(
        name,
        description,
        Priority.unsafe(42),
        Set(nxv.Schema),
        NonEmptyList.of(
          ProjectRef.unsafe("org", "project1"),
          ProjectRef.unsafe("org", "project2")
        ),
        UseCurrentCaller
      )
      ResolverValue.generateSource(nxv + "generated", crossProjectProject) shouldEqual
        jsonContentOf("resolvers/cross-project-current-caller-from-value.json")
    }
  }

}
