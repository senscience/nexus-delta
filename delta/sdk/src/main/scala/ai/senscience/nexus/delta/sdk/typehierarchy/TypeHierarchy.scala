package ai.senscience.nexus.delta.sdk.typehierarchy

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.TypeHierarchyResource
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchy.TypeHierarchyMapping
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchyCommand.{CreateTypeHierarchy, UpdateTypeHierarchy}
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchyEvent.{TypeHierarchyCreated, TypeHierarchyUpdated}
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchyRejection.{IncorrectRev, RevisionNotFound, TypeHierarchyAlreadyExists, TypeHierarchyDoesNotExist}
import ai.senscience.nexus.delta.sdk.typehierarchy.model.{TypeHierarchyCommand, TypeHierarchyEvent, TypeHierarchyRejection, TypeHierarchyState}
import ai.senscience.nexus.delta.sourcing.implicits.IriInstances.given
import ai.senscience.nexus.delta.sourcing.model.EntityType
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.{GlobalEntityDefinition, GlobalEventLog, StateMachine, Transactors}
import cats.effect.IO
import cats.effect.kernel.Clock

trait TypeHierarchy {

  /** Creates a type hierarchy with the provided mapping. */
  def create(mapping: TypeHierarchyMapping)(using Subject): IO[TypeHierarchyResource]

  /** Updates the type hierarchy with the provided mapping at the provided revision. */
  def update(mapping: TypeHierarchyMapping, rev: Int)(using Subject): IO[TypeHierarchyResource]

  /** Fetches the current type hierarchy. */
  def fetch: IO[TypeHierarchyResource]

  /** Fetches the type hierarchy at the provided revision. */
  def fetch(rev: Int): IO[TypeHierarchyResource]

}

object TypeHierarchy {

  final val entityType: EntityType = EntityType("type-hierarchy")
  final val typeHierarchyId: Iri   = nxv.TypeHierarchy

  private type TypeHierarchyLog =
    GlobalEventLog[Iri, TypeHierarchyState, TypeHierarchyCommand, TypeHierarchyEvent, TypeHierarchyRejection]

  def apply(xas: Transactors, config: TypeHierarchyConfig, clock: Clock[IO]): TypeHierarchy =
    apply(GlobalEventLog(definition(clock), config.eventLog, xas))

  def apply(log: TypeHierarchyLog): TypeHierarchy = new TypeHierarchy {
    override def create(mapping: TypeHierarchyMapping)(using subject: Subject): IO[TypeHierarchyResource] =
      eval(CreateTypeHierarchy(mapping, subject))

    override def update(mapping: TypeHierarchyMapping, rev: Int)(using subject: Subject): IO[TypeHierarchyResource] =
      eval(UpdateTypeHierarchy(mapping, rev, subject))

    override def fetch: IO[TypeHierarchyResource] =
      log.stateOr(typeHierarchyId, TypeHierarchyDoesNotExist).map(_.toResource)

    override def fetch(rev: Int): IO[TypeHierarchyResource] =
      log.stateOr(typeHierarchyId, rev, TypeHierarchyDoesNotExist, RevisionNotFound(_, _)).map(_.toResource)

    private def eval(cmd: TypeHierarchyCommand): IO[TypeHierarchyResource] =
      log.evaluate(typeHierarchyId, cmd).map(_.state.toResource)
  }

  private def evaluate(
      clock: Clock[IO]
  )(state: Option[TypeHierarchyState], command: TypeHierarchyCommand): IO[TypeHierarchyEvent] = {

    def create(c: CreateTypeHierarchy) =
      state match {
        case None => clock.realTimeInstant.map(TypeHierarchyCreated(c.mapping, 1, _, c.subject))
        case _    => IO.raiseError(TypeHierarchyAlreadyExists)
      }

    def update(c: UpdateTypeHierarchy) =
      state match {
        case None    => IO.raiseError(TypeHierarchyDoesNotExist)
        case Some(s) =>
          clock.realTimeInstant.map(TypeHierarchyUpdated(c.mapping, s.rev + 1, _, c.subject))
      }

    command match {
      case c: CreateTypeHierarchy => create(c)
      case u: UpdateTypeHierarchy => update(u)
    }
  }

  private def next(state: Option[TypeHierarchyState], ev: TypeHierarchyEvent): Option[TypeHierarchyState] =
    (state, ev) match {
      case (None, TypeHierarchyCreated(mapping, _, instant, subject))      =>
        Some(TypeHierarchyState(mapping, 1, deprecated = false, instant, subject, instant, subject))
      case (Some(s), TypeHierarchyUpdated(mapping, rev, instant, subject)) =>
        Some(s.copy(mapping = mapping, rev = rev, updatedAt = instant, updatedBy = subject))
      case (Some(_), TypeHierarchyCreated(_, _, _, _))                     => None
      case (None, _)                                                       => None
    }

  def definition(
      clock: Clock[IO]
  ): GlobalEntityDefinition[
    Iri,
    TypeHierarchyState,
    TypeHierarchyCommand,
    TypeHierarchyEvent,
    TypeHierarchyRejection
  ] =
    GlobalEntityDefinition(
      entityType,
      StateMachine(
        None,
        evaluate(clock),
        next
      ),
      TypeHierarchyEvent.serializer,
      TypeHierarchyState.serializer,
      onUniqueViolation = (_: Iri, c: TypeHierarchyCommand) =>
        c match {
          case _: CreateTypeHierarchy => TypeHierarchyAlreadyExists
          case u: UpdateTypeHierarchy => IncorrectRev(u.rev, u.rev + 1)
        }
    )

}
