package ai.senscience.nexus.delta.plugins.archive.model

import ai.senscience.nexus.delta.plugins.archive.model.ArchiveReference.{FileReference, FileSelfReference, ResourceReference}
import ai.senscience.nexus.delta.plugins.storage.storages.model.AbsolutePath
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.SerializationSuite
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.ResourceRepresentation
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef}
import cats.data.NonEmptySet
import org.http4s.syntax.literals.uri

import java.time.Instant

class ArchiveSerializationSuite extends SerializationSuite {

  private val id               = nxv + "archive"
  private val project          = ProjectRef.unsafe("myorg", "myproj")
  private val anotherProject   = ProjectRef.unsafe("myorg", "another")
  private val instant: Instant = Instant.EPOCH
  private val subject: Subject = User("username", Label.unsafe("myrealm"))

  private val absolutePath = AbsolutePath("/path/in/archive").toOption

  private val resourceId        = nxv + "resource"
  private val resourceReference =
    ResourceReference(
      ResourceRef.Revision(iri"$resourceId?rev=1", resourceId, 1),
      Some(anotherProject),
      absolutePath,
      Some(ResourceRepresentation.CompactedJsonLd)
    )

  private val fileSelfReference = FileSelfReference(uri"https://bbp.epfl.ch/nexus/org/proj/file", absolutePath)

  private val fileId        = nxv + "file"
  private val fileReference =
    FileReference(
      ResourceRef.Revision(iri"$fileId?rev=1", fileId, 1),
      Some(anotherProject),
      absolutePath
    )

  private val state = ArchiveState(
    id,
    project,
    NonEmptySet.of(resourceReference, fileSelfReference, fileReference),
    instant,
    subject
  )

  private val json = jsonContentOf("archives/database/state.json")

  test("Correctly serialize state") {
    ArchiveState.serializer.codec(state).equalsIgnoreArrayOrder(json)
  }

  test("Correctly deserialize") {
    assertEquals(ArchiveState.serializer.codec.decodeJson(json), Right(state))
  }

}
