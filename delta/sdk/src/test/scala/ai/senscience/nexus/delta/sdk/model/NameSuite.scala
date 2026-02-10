package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.sdk.error.FormatErrors.IllegalNameFormatError
import ai.senscience.nexus.testkit.mu.NexusSuite

class NameSuite extends NexusSuite {

  test("A Name should be constructed correctly from alphanumeric chars, - and _") {
    (1 to 128).toList.foreach { length =>
      val pool   = Vector.range('a', 'z') ++ Vector.range('0', '9') ++ Vector.range('A', 'Z') :+ '-' :+ '_' :+ ' '
      val string = genString(length, pool)
      assertEquals(Name.unsafe(string).value, string)
      Name(string).assertRight(Name.unsafe(string))
    }
  }

  test("A Name should fail to construct for illegal formats") {
    val cases = List("", "a ^", "è", "$", "%a", genString(129))
    cases.foreach { string =>
      Name(string).assertLeftOf[IllegalNameFormatError]
    }
  }

}
