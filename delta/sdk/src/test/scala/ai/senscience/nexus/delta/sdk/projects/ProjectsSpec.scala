package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schema, xsd}
import ai.senscience.nexus.delta.sdk.generators.{OrganizationGen, ProjectGen}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.organizations.FetchActiveOrganization
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.{OrganizationIsDeprecated, OrganizationNotFound}
import ai.senscience.nexus.delta.sdk.projects.Projects.evaluate
import ai.senscience.nexus.delta.sdk.projects.model.ProjectCommand.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectEvent.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.*
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, PrefixIri, ProjectFields}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.effect.{IO, Ref}

import java.time.Instant

class ProjectsSpec extends CatsEffectSpec {

  implicit val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  "The Projects state machine" when {
    val epoch                         = Instant.EPOCH
    val time2                         = Instant.ofEpochMilli(10L)
    val am                            = ApiMappings("xsd" -> xsd.base, "Person" -> schema.Person)
    val base                          = PrefixIri.unsafe(iri"http://example.com/base/")
    val vocab                         = PrefixIri.unsafe(iri"http://example.com/vocab/")
    val org1                          = OrganizationGen.state("org", 1)
    val org2                          = OrganizationGen.state("org2", 1, deprecated = true)
    val state                         = ProjectGen.state(
      "org",
      "proj",
      1,
      orgUuid = org1.uuid,
      description = Some("desc"),
      mappings = am,
      base = base.value,
      vocab = vocab.value,
      enforceSchema = true
    )
    val deprecatedState               = state.copy(deprecated = true)
    val label                         = state.label
    val uuid                          = state.uuid
    val orgLabel                      = state.organizationLabel
    val orgUuid                       = state.organizationUuid
    val desc                          = state.description
    val desc2                         = Some("desc2")
    val org2abel                      = org2.label
    val subject                       = User("myuser", label)
    val orgs: FetchActiveOrganization = {
      case `orgLabel` => IO.pure(org1.toResource.value)
      case `org2abel` => IO.raiseError(OrganizationIsDeprecated(org2abel))
      case label      => IO.raiseError(OrganizationNotFound(label))
    }

    val ref              = ProjectRef(orgLabel, label)
    val ref2             = ProjectRef(org2abel, label)
    val ref2IsReferenced = ProjectIsReferenced(ref, Map(ref -> Set(nxv + "ref1")))

    val createdProjects                  = Ref.unsafe[IO, Set[ProjectRef]](Set.empty)
    def onCreateRef(project: ProjectRef) = createdProjects.update(_ + project)

    val validateDeletion: ValidateProjectDeletion = {
      case `ref`  => IO.unit
      case `ref2` => IO.raiseError(ref2IsReferenced)
      case _      => IO.raiseError(new IllegalArgumentException(s"Only '$ref' and '$ref2' are expected here"))
    }

    implicit val uuidF: UUIDF = UUIDF.fixed(uuid)

    "evaluating an incoming command" should {

      val eval   = evaluate(orgs, onCreateRef, validateDeletion, clock)(_, _)
      val fields = ProjectFields(desc, am, base, vocab, enforceSchema = true)

      "create a new create event" in {
        eval(None, CreateProject(ref, fields, subject)).accepted shouldEqual
          ProjectCreated(label, uuid, orgLabel, orgUuid, 1, desc, am, base, vocab, enforceSchema = true, epoch, subject)
        createdProjects.get.accepted should contain(ref)
      }

      "create a new update event" in {
        eval(Some(state), UpdateProject(ref, fields, 1, subject)).accepted shouldEqual
          ProjectUpdated(label, uuid, orgLabel, orgUuid, 2, desc, am, base, vocab, enforceSchema = true, epoch, subject)
      }

      "create a new deprecate event" in {
        eval(Some(state), DeprecateProject(ref, 1, subject)).accepted shouldEqual
          ProjectDeprecated(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
      }

      "create a new undeprecation event" in {
        eval(Some(deprecatedState), UndeprecateProject(ref, 1, subject)).accepted shouldEqual
          ProjectUndeprecated(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
      }

      "create a new deletion event" in {
        eval(Some(state), DeleteProject(ref, 1, subject)).accepted shouldEqual
          ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
      }

      "reject with IncorrectRev" in {
        val list = List(
          state           -> UpdateProject(ref, fields, 2, subject),
          state           -> DeprecateProject(ref, 2, subject),
          deprecatedState -> UndeprecateProject(ref, 2, subject)
        )
        forAll(list) { case (state, cmd) =>
          eval(Some(state), cmd).rejectedWith[IncorrectRev]
        }
      }

      "reject with OrganizationIsDeprecated" in {
        val list = List(
          None                  -> CreateProject(ref2, fields, subject),
          Some(state)           -> UpdateProject(ref2, fields, 1, subject),
          Some(state)           -> DeprecateProject(ref2, 1, subject),
          Some(deprecatedState) -> UndeprecateProject(ref2, 1, subject)
        )
        forAll(list) { case (state, cmd) =>
          eval(state, cmd).rejected shouldEqual OrganizationIsDeprecated(ref2.organization)
        }
      }

      "reject with OrganizationNotFound" in {
        val orgNotFound = ProjectRef(label, Label.unsafe("other"))
        val list        = List(
          None                  -> CreateProject(orgNotFound, fields, subject),
          Some(state)           -> UpdateProject(orgNotFound, fields, 1, subject),
          Some(state)           -> DeprecateProject(orgNotFound, 1, subject),
          Some(deprecatedState) -> UndeprecateProject(orgNotFound, 1, subject)
        )
        forAll(list) { case (state, cmd) =>
          eval(state, cmd).rejected shouldEqual OrganizationNotFound(label)
        }
      }

      "reject with ProjectIsDeprecated" in {
        val list = List(
          deprecatedState -> UpdateProject(ref, fields, 1, subject),
          deprecatedState -> DeprecateProject(ref, 1, subject)
        )
        forAll(list) { case (state, cmd) =>
          eval(Some(state), cmd).rejectedWith[ProjectIsDeprecated]
        }
      }

      "reject with ProjectIsNotDeprecated" in {
        eval(Some(state), UndeprecateProject(ref, 1, subject)).rejectedWith[ProjectIsNotDeprecated]
      }

      "reject with ProjectNotFound" in {
        val list = List(
          None -> UpdateProject(ref, fields, 1, subject),
          None -> DeprecateProject(ref, 1, subject),
          None -> DeleteProject(ref, 1, subject),
          None -> UndeprecateProject(ref, 1, subject)
        )
        forAll(list) { case (state, cmd) =>
          eval(state, cmd).rejectedWith[ProjectNotFound]
        }
      }

      "reject with ProjectAlreadyExists" in {
        eval(Some(state), CreateProject(ref, fields, subject)).rejectedWith[ProjectAlreadyExists]
      }

      "do not reject with ProjectIsDeprecated" in {
        val cmd = DeleteProject(ref, 1, subject)
        eval(Some(deprecatedState), cmd).accepted shouldEqual
          ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
      }

      "reject with ProjectIsReferenced" in {
        eval(Some(state), DeleteProject(ref2, 1, subject)).rejected shouldEqual
          ref2IsReferenced
      }
    }

    "producing next state" should {
      val next = Projects.next _

      "create a new ProjectCreated state" in {
        next(
          None,
          ProjectCreated(label, uuid, orgLabel, orgUuid, 1, desc, am, base, vocab, enforceSchema = true, time2, subject)
        ).value shouldEqual
          state.copy(createdAt = time2, createdBy = subject, updatedAt = time2, updatedBy = subject)

        next(
          Some(state),
          ProjectCreated(label, uuid, orgLabel, orgUuid, 1, desc, am, base, vocab, enforceSchema = true, time2, subject)
        ) shouldEqual None
      }

      "create a new ProjectUpdated state" in {
        next(
          None,
          ProjectUpdated(
            label,
            uuid,
            orgLabel,
            orgUuid,
            2,
            desc2,
            ApiMappings.empty,
            base,
            vocab,
            enforceSchema = false,
            time2,
            subject
          )
        ) shouldEqual None

        next(
          Some(state),
          ProjectUpdated(
            label,
            uuid,
            orgLabel,
            orgUuid,
            2,
            desc2,
            ApiMappings.empty,
            base,
            vocab,
            enforceSchema = false,
            time2,
            subject
          )
        ).value shouldEqual
          state.copy(
            rev = 2,
            description = desc2,
            apiMappings = ApiMappings.empty,
            enforceSchema = false,
            updatedAt = time2,
            updatedBy = subject
          )
      }

      "create new ProjectDeprecated state" in {
        next(
          None,
          ProjectDeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
        ) shouldEqual None

        next(
          Some(state),
          ProjectDeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
        ).value shouldEqual
          state.copy(rev = 2, deprecated = true, updatedAt = time2, updatedBy = subject)
      }

      "create new ProjectMarkedForDeletion state" in {
        next(None, ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, time2, subject)) shouldEqual None

        next(Some(state), ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, time2, subject)).value shouldEqual
          state.copy(rev = 2, markedForDeletion = true, updatedAt = time2, updatedBy = subject)
      }

      "create new ProjectUndeprecated state" in {
        next(
          None,
          ProjectUndeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
        ) shouldEqual None

        next(
          Some(deprecatedState),
          ProjectUndeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
        ).value shouldEqual
          deprecatedState.copy(rev = 2, deprecated = false, updatedAt = time2, updatedBy = subject)
      }
    }
  }
}
