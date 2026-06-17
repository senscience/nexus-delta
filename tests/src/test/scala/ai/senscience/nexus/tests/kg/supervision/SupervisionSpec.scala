package ai.senscience.nexus.tests.kg.supervision

import ai.senscience.nexus.tests.BaseIntegrationSpec
import ai.senscience.nexus.tests.Identity.{Anonymous, ServiceAccount}
import io.circe.Json

class SupervisionSpec extends BaseIntegrationSpec {

  "The supervision endpoint" should {
    "reject calls without 'supervision/read' permission" in {
      deltaClient.get[Json]("/supervision/projections", Anonymous) { expectForbidden }
    }

    "accept calls with 'supervision/read'" in {
      deltaClient.get[Json]("/supervision/projections", ServiceAccount) { expectOk }
    }
  }

}
