package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectDeletionIsDisabled, ProjectIsReferenced}
import ai.senscience.nexus.delta.sourcing.model.EntityDependency.ReferencedBy
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO

class ValidateProjectDeletionSuite extends NexusSuite {

  private val noReferences   = ProjectRef.unsafe("org", "no-refs")
  private val withReferences = ProjectRef.unsafe("org", "has-refs")

  private val references = Set(
    ReferencedBy(noReferences, nxv + "ref1"),
    ReferencedBy(noReferences, nxv + "ref2")
  )

  private val deletionEnabled = ValidateProjectDeletion(
    fetchReferences = {
      case `noReferences`   => IO.pure(Set.empty)
      case `withReferences` => IO.pure(references)
      case _                => IO.raiseError(new IllegalStateException(s"Only '$noReferences', '$withReferences' are accepted"))
    },
    enabled = true
  )

  private val deletionDisabled = ValidateProjectDeletion(
    _ => IO.raiseError(new IllegalStateException("Should never be called as deletion is disabled.")),
    enabled = false
  )

  test("Pass if no reference is returned") {
    deletionEnabled.apply(noReferences)
  }

  test("Fail if a reference is detected") {
    deletionEnabled
      .apply(withReferences)
      .interceptEquals(ProjectIsReferenced(withReferences, Map(noReferences -> Set(nxv + "ref1", nxv + "ref2"))))
  }

  test("Fail as project deletion is disabled") {
    deletionDisabled
      .apply(noReferences)
      .interceptEquals(ProjectDeletionIsDisabled)
  }

}
