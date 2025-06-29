package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.plugins.archive.model.ArchiveReference.ResourceReference
import ai.senscience.nexus.delta.plugins.archive.model.{ArchiveState, ArchiveValue, CreateArchive}
import ai.senscience.nexus.delta.plugins.storage.storages.model.AbsolutePath
import ai.senscience.nexus.delta.sdk.model.ResourceRepresentation.SourceJson
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.data.NonEmptySet

import java.nio.file.Paths
import java.time.Instant

class ArchivesSTMSpec extends CatsEffectSpec {

  "An Archive STM" when {
    val id      = iri"http://localhost${genString()}"
    val project = ProjectRef(Label.unsafe("org"), Label.unsafe("proj"))
    val res     = ResourceReference(
      ref = ResourceRef.Latest(id),
      project = Some(project),
      path = Some(AbsolutePath(Paths.get("/resource.json")).rightValue),
      representation = Some(SourceJson)
    )
    val bob     = User("bob", Label.unsafe("realm"))

    "evaluating a command" should {
      "create a new archive" in {
        val command = CreateArchive(
          id = id,
          project = project,
          value = ArchiveValue.unsafe(NonEmptySet.of(res)),
          subject = bob
        )
        val event   = ArchiveState(
          id = id,
          project = project,
          resources = NonEmptySet.of(res),
          createdAt = Instant.EPOCH,
          createdBy = bob
        )
        Archives.evaluate(clock)(command).accepted shouldEqual event
      }
    }
  }

}
