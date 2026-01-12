package ai.senscience.nexus.delta.rdf.shacl

import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.Json

class ValidationReportSuite extends NexusSuite {

  test("Be non conform") {
    val report = ValidationReport.unsafe(true, 0, Json.obj())
    assertEquals(report.conformsWithTargetedNodes, false)
  }
}
