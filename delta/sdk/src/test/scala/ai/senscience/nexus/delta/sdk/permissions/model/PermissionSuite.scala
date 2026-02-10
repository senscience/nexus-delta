package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.sdk.error.FormatErrors.IllegalPermissionFormatError
import ai.senscience.nexus.delta.sdk.permissions.Permissions.acls
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.Json
import io.circe.syntax.*

import scala.util.Random

class PermissionSuite extends NexusSuite {

  test("A Permission should be constructed correctly for valid strings") {
    for _ <- 1 to 100 do {
      val valid = genValid
      Permission(valid).assertRight(Permission.unsafe(valid))
    }
  }

  test("A Permission should fail to construct for illegal strings") {
    List("", " ", "1", "1abd", "_abd", "foö", "bar*", genString(33)).foreach { string =>
      Permission(string).assertLeftOf[IllegalPermissionFormatError]
    }
  }

  test("A Permission should be converted to Json") {
    assertEquals(acls.read.asJson, Json.fromString(acls.read.value))
  }

  test("A Permission should be constructed from Json") {
    Json.fromString(acls.read.value).as[Permission].assertRight(acls.read)
  }

  private def genValid: String = {
    val lower   = 'a' to 'z'
    val upper   = 'A' to 'Z'
    val numbers = '0' to '9'
    val symbols = List('-', '_', ':', '\\', '/')
    val head    = genString(1, lower ++ upper)
    val tail    = genString(Random.nextInt(32), lower ++ upper ++ numbers ++ symbols)
    head + tail
  }

}
