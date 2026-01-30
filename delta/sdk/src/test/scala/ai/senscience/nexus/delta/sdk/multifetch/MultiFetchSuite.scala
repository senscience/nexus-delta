package ai.senscience.nexus.delta.sdk.multifetch

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.ResourceRepresentation
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchRequest.Input
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchResponse.Result.{AuthorizationFailed, NotFound, Success}
import ai.senscience.nexus.delta.sdk.multifetch.model.{MultiFetchRequest, MultiFetchResponse}
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import cats.effect.IO

class MultiFetchSuite extends NexusSuite with Fixtures {

  private given subject: Subject = Identity.User("user", Label.unsafe("realm"))
  private given caller: Caller   = Caller(subject)

  private val project1 = ProjectRef.unsafe("org", "proj1")
  private val project2 = ProjectRef.unsafe("org", "proj2")

  private val permissions = Set(Permissions.resources.read)
  private val aclCheck    = AclSimpleCheck((subject, project1, permissions)).accepted

  private val successId      = nxv + "success"
  private val successContent =
    ResourceGen.jsonLdContent(successId, project1, jsonContentOf("resources/resource.json", "id" -> successId))
  private val notFoundId     = nxv + "not-found"
  private val unauthorizedId = nxv + "unauthorized"

  private def fetchResource =
    (input: MultiFetchRequest.Input) => {
      input match {
        case MultiFetchRequest.Input(Latest(`successId`), `project1`) =>
          IO.pure(Some(successContent))
        case _                                                        => IO.none
      }
    }

  private val multiFetch = MultiFetch(
    aclCheck,
    fetchResource
  )

  private val request = MultiFetchRequest(
    ResourceRepresentation.NTriples,
    Input(Latest(successId), project1),
    Input(Latest(notFoundId), project1),
    Input(Latest(unauthorizedId), project2)
  )

  test("Return the response matching the user acls") {

    val expected = MultiFetchResponse(
      ResourceRepresentation.NTriples,
      NonEmptyList.of(
        Success(Latest(successId), project1, successContent),
        NotFound(Latest(notFoundId), project1),
        AuthorizationFailed(Latest(unauthorizedId), project2)
      )
    )

    multiFetch(request).assertEquals(expected)
  }

  test("Return only unauthorized for a user with no access") {
    val expected = MultiFetchResponse(
      ResourceRepresentation.NTriples,
      NonEmptyList.of(
        AuthorizationFailed(Latest(successId), project1),
        AuthorizationFailed(Latest(notFoundId), project1),
        AuthorizationFailed(Latest(unauthorizedId), project2)
      )
    )

    multiFetch(request)(using Caller.Anonymous).assertEquals(expected)
  }

}
