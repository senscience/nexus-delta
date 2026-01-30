package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig.PrefixConfig
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig.PrefixConfig.PrefixIriTemplate
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.mu.NexusSuite

class ProjectFieldsSuite extends NexusSuite {

  private val project = ProjectRef.unsafe("org", "proj")

  private val prefixIriConfig = PrefixConfig(
    PrefixIriTemplate.unsafe("https://localhost/base/{{org}}/{{project}}/"),
    PrefixIriTemplate.unsafe("https://localhost/vocab/{{org}}/{{project}}/")
  )

  private val decoder = ProjectFields.decoder(project, prefixIriConfig)

  test("Decode with all default values") {
    val json = json""" { }"""

    val expectedBase  = iri"https://localhost/base/$project/"
    val expectedVocab = iri"https://localhost/vocab/$project/"

    assertEquals(
      decoder.decodeJson(json),
      Right(
        ProjectFields(
          None,
          ApiMappings.empty,
          PrefixIri.unsafe(expectedBase),
          PrefixIri.unsafe(expectedVocab),
          enforceSchema = false
        )
      )
    )
  }

  test("Decode with provided base and vocab values") {
    val customBase  = iri"https://localhost/custom-base/"
    val customVocab = iri"https://localhost/custom-vocab/"
    val json        = json""" { "base":  "$customBase", "vocab":  "$customVocab" }"""

    assertEquals(
      decoder.decodeJson(json),
      Right(
        ProjectFields(
          None,
          ApiMappings.empty,
          PrefixIri.unsafe(customBase),
          PrefixIri.unsafe(customVocab),
          enforceSchema = false
        )
      )
    )

  }
}
