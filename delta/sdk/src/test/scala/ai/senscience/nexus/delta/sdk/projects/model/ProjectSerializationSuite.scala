package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.SerializationSuite
import ai.senscience.nexus.delta.sdk.projects.model.ProjectEvent.*
import ai.senscience.nexus.delta.sdk.sse.SseEncoder.SseData
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}

import java.time.Instant
import java.util.UUID

class ProjectSerializationSuite extends SerializationSuite {

  private val sseEncoder = ProjectEvent.sseEncoder

  private val instant: Instant = Instant.EPOCH
  private val rev: Int         = 1

  private val realm: Label     = Label.unsafe("myrealm")
  private val subject: Subject = User("username", realm)

  private val org: Label          = Label.unsafe("myorg")
  private val orgUuid: UUID       = UUID.fromString("b6bde92f-7836-4da6-8ead-2e0fd516ebe7")
  private val description: String = "some description"

  private val proj: Label              = Label.unsafe("myproj")
  private val projUuid: UUID           = UUID.fromString("fe1301a6-a105-4966-84af-32723fd003d2")
  private val apiMappings: ApiMappings = ApiMappings("nxv" -> nxv.base)
  private val base: PrefixIri          = PrefixIri.unsafe(schemas.base)
  private val vocab: PrefixIri         = PrefixIri.unsafe(nxv.base)

  private val created    =
    ProjectCreated(
      label = proj,
      uuid = projUuid,
      organizationLabel = org,
      organizationUuid = orgUuid,
      rev = rev,
      description = Some(description),
      apiMappings = apiMappings,
      base = base,
      vocab = vocab,
      enforceSchema = true,
      instant = instant,
      subject = subject
    )
  private val updated    =
    ProjectUpdated(
      label = proj,
      uuid = projUuid,
      organizationLabel = org,
      organizationUuid = orgUuid,
      rev = rev,
      description = Some(description),
      apiMappings = apiMappings,
      base = base,
      vocab = vocab,
      enforceSchema = true,
      instant = instant,
      subject = subject
    )
  private val deprecated =
    ProjectDeprecated(
      label = proj,
      uuid = projUuid,
      organizationLabel = org,
      organizationUuid = orgUuid,
      rev = rev,
      instant = instant,
      subject = subject
    )

  private val undeprecated =
    ProjectUndeprecated(
      label = proj,
      uuid = projUuid,
      organizationLabel = org,
      organizationUuid = orgUuid,
      rev = rev,
      instant = instant,
      subject = subject
    )

  private val projectsMapping = List(
    (created, loadEvents("projects", "project-created.json")),
    (updated, loadEvents("projects", "project-updated.json")),
    (deprecated, loadEvents("projects", "project-deprecated.json")),
    (undeprecated, loadEvents("projects", "project-undeprecated.json"))
  )

  projectsMapping.foreach { case (event, (database, sse)) =>
    test(s"Correctly serialize ${event.getClass.getName}") {
      assertOutput(ProjectEvent.serializer, event, database)
    }

    test(s"Correctly deserialize ${event.getClass.getName}") {
      assertEquals(ProjectEvent.serializer.codec.decodeJson(database), Right(event))
    }

    test(s"Correctly serialize ${event.getClass.getName} as an SSE") {
      sseEncoder.toSse
        .decodeJson(database)
        .assertRight(SseData(ClassUtils.simpleName(event), Some(ProjectRef(org, proj)), sse))
    }
  }

  private val state = ProjectState(
    proj,
    projUuid,
    org,
    orgUuid,
    rev = rev,
    deprecated = false,
    markedForDeletion = false,
    description = Some(description),
    apiMappings = apiMappings,
    base = ProjectBase(base.value),
    vocab = vocab.value,
    enforceSchema = true,
    createdAt = instant,
    createdBy = subject,
    updatedAt = instant,
    updatedBy = subject
  )

  private val jsonState = jsonContentOf("projects/project-state.json")

  test(s"Correctly serialize a ProjectState") {
    assertOutput(ProjectState.serializer, state, jsonState)
  }

  test(s"Correctly deserialize a ProjectState") {
    assertEquals(ProjectState.serializer.codec.decodeJson(jsonState), Right(state))
  }

}
