package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.kernel.error.ThrowableValue
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, rdfs, schemas}
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.{IriOrBNode, Vocabulary}
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestCommand.*
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestEvent.{PullRequestCreated, PullRequestMerged, PullRequestTagged, PullRequestUpdated}
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestRejection.{AlreadyExists, NotFound, PullRequestAlreadyClosed}
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.{PullRequestActive, PullRequestClosed}
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.event.Event.ScopedEvent
import ai.senscience.nexus.delta.sourcing.event.ScopedEventStore
import ai.senscience.nexus.delta.sourcing.model.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import ai.senscience.nexus.delta.sourcing.state.{GraphResource, ScopedStateStore}
import cats.effect.IO
import cats.syntax.all.*
import doobie.syntax.all.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.syntax.KeyOps
import io.circe.{Codec, Json}

import java.time.Instant

object PullRequest {

  type EventStore = ScopedEventStore[Iri, PullRequestEvent]

  val entityType: EntityType = EntityType("merge-request")

  def eventStore(queryConfig: QueryConfig): ScopedEventStore[Iri, PullRequestEvent] =
    ScopedEventStore[Iri, PullRequestEvent](
      PullRequest.entityType,
      PullRequestEvent.serializer,
      queryConfig
    )

  def stateStore(xas: Transactors, queryConfig: QueryConfig): ScopedStateStore[Iri, PullRequestState] =
    ScopedStateStore(
      PullRequest.entityType,
      PullRequestState.serializer,
      queryConfig,
      xas
    )

  def eventStore(xas: Transactors, queryConfig: QueryConfig, populateWith: PullRequestEvent*): IO[EventStore] = {
    val store = eventStore(queryConfig)
    populateWith.traverse(store.save).transact(xas.write).as(store)
  }

  val stateMachine: StateMachine[PullRequestState, PullRequestCommand, PullRequestEvent] =
    StateMachine(
      None,
      (state: Option[PullRequestState], command: PullRequestCommand) =>
        state.fold[IO[PullRequestEvent]] {
          command match {
            case Create(id, project) => IO.pure(PullRequestCreated(id, project, Instant.EPOCH, Anonymous))
            case _                   => IO.raiseError(NotFound)
          }
        } { s =>
          (s, command) match {
            case (_, Create(id, project))                                   => IO.raiseError(AlreadyExists(id, project))
            case (_: PullRequestActive, Update(id, project, rev))           =>
              IO.pure(PullRequestUpdated(id, project, rev, Instant.EPOCH, Anonymous))
            case (_: PullRequestActive, TagPR(id, project, rev, targetRev)) =>
              IO.pure(PullRequestTagged(id, project, rev, targetRev, Instant.EPOCH, Anonymous))
            case (_: PullRequestActive, Merge(id, project, rev))            =>
              IO.pure(PullRequestMerged(id, project, rev, Instant.EPOCH, Anonymous))
            case (_, Boom(_, _, message))                                   => IO.raiseError(new RuntimeException(message))
            case (_, _: Never)                                              => IO.never
            case (_: PullRequestClosed, _)                                  => IO.raiseError(PullRequestAlreadyClosed(command.id, command.project))
          }
        },
      (state: Option[PullRequestState], event: PullRequestEvent) =>
        state.fold[Option[PullRequestState]] {
          event match {
            case PullRequestCreated(id, project, instant, subject) =>
              Some(PullRequestActive(id, project, 1, instant, subject, instant, subject))
            case _                                                 => None
          }
        } { s =>
          (s, event) match {
            case (_, _: PullRequestCreated)                                                 => None
            case (po: PullRequestActive, PullRequestUpdated(_, _, rev, instant, subject))   =>
              Some(po.copy(rev = rev, updatedAt = instant, updatedBy = subject))
            case (po: PullRequestActive, PullRequestTagged(_, _, rev, _, instant, subject)) =>
              Some(po.copy(rev = rev, updatedAt = instant, updatedBy = subject))
            case (po: PullRequestActive, PullRequestMerged(_, _, rev, instant, subject))    =>
              Some(PullRequestClosed(po.id, po.project, rev, po.createdAt, po.createdBy, instant, subject))
            case (_: PullRequestClosed, _)                                                  => None
          }
        }
    )

  sealed trait PullRequestCommand extends Product with Serializable {
    def id: Iri
    def project: ProjectRef
  }

  object PullRequestCommand {
    final case class Create(id: Iri, project: ProjectRef)                          extends PullRequestCommand
    final case class Update(id: Iri, project: ProjectRef, rev: Int)                extends PullRequestCommand
    final case class TagPR(id: Iri, project: ProjectRef, rev: Int, targetRev: Int) extends PullRequestCommand
    final case class Merge(id: Iri, project: ProjectRef, rev: Int)                 extends PullRequestCommand

    final case class Boom(id: Iri, project: ProjectRef, message: String) extends PullRequestCommand
    final case class Never(id: Iri, project: ProjectRef)                 extends PullRequestCommand
  }

  sealed trait PullRequestEvent extends ScopedEvent {
    def id: Iri
  }

  object PullRequestEvent {
    final case class PullRequestCreated(id: Iri, project: ProjectRef, instant: Instant, subject: Subject)
        extends PullRequestEvent {
      override val rev: Int = 1
    }

    final case class PullRequestUpdated(id: Iri, project: ProjectRef, rev: Int, instant: Instant, subject: Subject)
        extends PullRequestEvent

    final case class PullRequestTagged(
        id: Iri,
        project: ProjectRef,
        rev: Int,
        targetRev: Int,
        instant: Instant,
        subject: Subject
    ) extends PullRequestEvent

    final case class PullRequestMerged(id: Iri, project: ProjectRef, rev: Int, instant: Instant, subject: Subject)
        extends PullRequestEvent

    val serializer: Serializer[Iri, PullRequestEvent] = {
      import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
      implicit val configuration: Configuration            = Configuration.default.withDiscriminator("@type")
      implicit val coder: Codec.AsObject[PullRequestEvent] = deriveConfiguredCodec[PullRequestEvent]
      Serializer()
    }
  }

  sealed trait PullRequestRejection extends ThrowableValue

  object PullRequestRejection {
    final case object NotFound                                              extends PullRequestRejection
    final case object TagNotFound                                           extends PullRequestRejection
    final case class RevisionNotFound(provided: Int, current: Int)          extends PullRequestRejection
    final case class AlreadyExists(id: Iri, project: ProjectRef)            extends PullRequestRejection
    final case class PullRequestAlreadyClosed(id: Iri, project: ProjectRef) extends PullRequestRejection
  }

  sealed trait PullRequestState extends ScopedState {
    def id: Iri
    def project: ProjectRef
    def rev: Int
    def tags: Tags
    def createdAt: Instant
    def createdBy: Subject
    def updatedAt: Instant
    def updatedBy: Subject

    override def schema: ResourceRef = Latest(schemas + "pull-request.json")

    override def types: Set[IriOrBNode.Iri] = Set(nxv + "PullRequest")

    def graph: Graph = this match {
      case _: PullRequestActive =>
        Graph
          .empty(id)
          .add(Vocabulary.rdf.tpe, nxv + "PullRequest")
          .add(nxv + "status", "active")
          .add(rdfs.label, "active")
      case _: PullRequestClosed =>
        Graph
          .empty(id)
          .add(Vocabulary.rdf.tpe, nxv + "PullRequest")
          .add(nxv + "status", "closed")
          .add(rdfs.label, "closed")
    }

    def metadataGraph(base: Iri): Graph = {
      def subject(subject: Subject): Iri = subject match {
        case Identity.Anonymous            => base / "anonymous"
        case Identity.User(subject, realm) => base / "realms" / realm.value / "users" / subject
      }
      Graph
        .empty(id)
        .add(nxv.project.iri, project.toString)
        .add(nxv.rev.iri, rev)
        .add(nxv.deprecated.iri, deprecated)
        .add(nxv.createdAt.iri, createdAt)
        .add(nxv.createdBy.iri, subject(createdBy))
        .add(nxv.updatedAt.iri, updatedAt)
        .add(nxv.updatedBy.iri, subject(updatedBy))
    }

    def source: Json = this match {
      case p: PullRequestActive =>
        Json.obj(
          "@id"    := id.toString,
          "@type"  := p.types,
          "status" := "active",
          "label"  := "active"
        )
      case _: PullRequestClosed =>
        Json.obj(
          "@id"    := id,
          "@type"  := "PullRequest",
          "status" := "closed",
          "label"  := "closed"
        )
    }
  }

  object PullRequestState {

    final case class PullRequestActive(
        id: Iri,
        project: ProjectRef,
        rev: Int,
        createdAt: Instant,
        createdBy: Subject,
        updatedAt: Instant,
        updatedBy: Subject,
        override val types: Set[Iri] = Set(nxv + "PullRequest")
    ) extends PullRequestState {
      override def deprecated: Boolean = false

      override def tags: Tags = Tags.empty
    }

    final case class PullRequestClosed(
        id: Iri,
        project: ProjectRef,
        rev: Int,
        createdAt: Instant,
        createdBy: Subject,
        updatedAt: Instant,
        updatedBy: Subject
    ) extends PullRequestState {

      override def tags: Tags = Tags(UserTag.unsafe("closed") -> rev)

      override def deprecated: Boolean = true
    }

    implicit val serializer: Serializer[Iri, PullRequestState] = {
      import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
      implicit val configuration: Configuration            = Configuration.default.withDiscriminator("@type")
      implicit val coder: Codec.AsObject[PullRequestState] = deriveConfiguredCodec[PullRequestState]
      Serializer.dropNullsInjectType()
    }

    def toGraphResource(state: PullRequestState, base: Iri): GraphResource =
      GraphResource(
        PullRequest.entityType,
        state.project,
        state.id,
        state.rev,
        state.deprecated,
        state.schema,
        state.types,
        state.graph,
        state.metadataGraph(base),
        state.source
      )
  }

}
