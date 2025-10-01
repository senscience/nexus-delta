package ai.senscience.nexus.delta.kernel

import munit.FunSuite

class SecretSuite extends FunSuite {

  test("Secret should be mapped") {
    assertEquals(Secret("value").map(_.toUpperCase), Secret("VALUE"))
  }

  test("Secret should not expose its value when calling toString") {
    assertEquals(Secret("value").toString, "SECRET")
  }

}
