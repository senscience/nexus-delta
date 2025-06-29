package ai.senscience.nexus.delta.sdk.instances

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.implicits.*
import munit.FunSuite

class ProjectRefInstanceSuite extends FunSuite {

  private val projectRef1 = ProjectRef.unsafe("org1", "projA")
  private val projectRef2 = ProjectRef.unsafe("org1", "projB")
  private val projectRef3 = ProjectRef.unsafe("org2", "projA")

  test(s"$projectRef1 should come before $projectRef2") {
    assert(projectRef1 < projectRef2)
  }

  test(s"$projectRef2 should come before $projectRef3") {
    assert(projectRef2 < projectRef3)
  }

}
