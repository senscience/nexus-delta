package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.model.ResourceAccess.{EphemeralAccess, InProjectAccess}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import org.http4s.syntax.literals.uri

class ResourceAccessSuite extends NexusSuite {

  private given BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val projectRef = ProjectRef.unsafe("myorg", "myproject")

  test("ResourceAccess should be constructed for permissions") {
    val expected = uri"http://localhost/v1/permissions"
    assertEquals(ResourceAccess.permissions.uri, expected)
  }

  test("ResourceAccess should be constructed for acls") {
    val org  = Label.unsafe("org")
    val proj = Label.unsafe("project")
    val list =
      List(
        ResourceAccess.acl(AclAddress.Root)               -> uri"http://localhost/v1/acls",
        ResourceAccess.acl(org)                           -> uri"http://localhost/v1/acls/org",
        ResourceAccess.acl(AclAddress.Project(org, proj)) -> uri"http://localhost/v1/acls/org/project"
      )
    list.foreach { case (resourceUris, expected) =>
      assertEquals(resourceUris.uri, expected)
    }
  }

  test("ResourceAccess should be constructed for realms") {
    val myrealm      = Label.unsafe("myrealm")
    val expected     = uri"http://localhost/v1/realms/myrealm"
    val resourceUris = ResourceAccess.realm(myrealm)
    assertEquals(resourceUris.uri, expected)
  }

  test("ResourceAccess should be constructed for organizations") {
    val expected     = uri"http://localhost/v1/orgs/myorg"
    val resourceUris = ResourceAccess.organization(projectRef.organization)
    assertEquals(resourceUris.uri, expected)
  }

  test("ResourceAccess should be constructed for projects") {
    val expected     = uri"http://localhost/v1/projects/myorg/myproject"
    val resourceUris = ResourceAccess.project(projectRef)
    assertEquals(resourceUris.uri, expected)
  }

  test("ResourceAccess should be constructed for schemas") {
    val id       = schemas + "myid"
    val expected = uri"http://localhost/v1/schemas/myorg/myproject/" / id.toString

    val resourceUris = ResourceAccess.schema(projectRef, id).asInstanceOf[InProjectAccess]

    assertEquals(resourceUris.uri, expected)
    assertEquals(resourceUris.project, projectRef)
  }

  test("ResourceAccess should be constructed for resolvers") {
    val id       = nxv + "myid"
    val expected = uri"http://localhost/v1/resolvers/myorg/myproject" / id.toString

    val resourceUris = ResourceAccess.resolver(projectRef, id).asInstanceOf[InProjectAccess]
    assertEquals(resourceUris.uri, expected)
    assertEquals(resourceUris.project, projectRef)
  }

  test("ResourceAccess should be constructed for ephemeral resources") {
    val id       = nxv + "myid"
    val expected = uri"http://localhost/v1/archives/myorg/myproject" / id.toString

    val resourceUris = ResourceAccess.ephemeral("archives", projectRef, id)

    resourceUris match {
      case v: EphemeralAccess =>
        assertEquals(v.uri, expected)
        assertEquals(v.project, projectRef)
      case other              =>
        fail(
          s"Expected type 'EphemeralResourceInProjectUris', but got value '$other' of type '${other.getClass.getCanonicalName}'"
        )
    }
  }
}
