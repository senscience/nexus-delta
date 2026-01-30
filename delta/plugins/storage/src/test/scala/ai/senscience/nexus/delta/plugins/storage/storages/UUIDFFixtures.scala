package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.kernel.utils.UUIDF

import java.util.UUID

object UUIDFFixtures {
  trait Random {
    val randomUuid: UUID          = UUID.randomUUID()
    given fixedRandomUuidF: UUIDF = UUIDF.fixed(randomUuid)
  }

  trait Fixed {
    val fixedUuid: UUID     = UUID.fromString("8049ba90-7cc6-4de5-93a1-802c04200dcc")
    given fixedUuidF: UUIDF = UUIDF.fixed(fixedUuid)
  }
}
