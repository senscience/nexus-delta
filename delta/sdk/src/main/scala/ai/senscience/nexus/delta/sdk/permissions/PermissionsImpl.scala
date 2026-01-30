package ai.senscience.nexus.delta.sdk.permissions

import ai.senscience.nexus.delta.sdk.PermissionsResource
import ai.senscience.nexus.delta.sdk.permissions.Permissions.labelId
import ai.senscience.nexus.delta.sdk.permissions.PermissionsImpl.PermissionsLog
import ai.senscience.nexus.delta.sdk.permissions.model.*
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsCommand.*
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsRejection.{RevisionNotFound, UnexpectedState}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.{GlobalEventLog, Transactors}
import cats.effect.{Clock, IO}
import org.typelevel.otel4s.trace.Tracer

final class PermissionsImpl private (
    override val minimum: Set[Permission],
    log: PermissionsLog
)(using Tracer[IO])
    extends Permissions {

  private val initial = PermissionsState.initial(minimum)

  override def fetch: IO[PermissionsResource] =
    log
      .stateOr[PermissionsRejection](labelId, UnexpectedState)
      .handleErrorWith(_ => IO.pure(initial))
      .map(_.toResource(minimum))
      .surround("fetchPermissions")

  override def fetchAt(rev: Int): IO[PermissionsResource] =
    log
      .stateOr(
        labelId,
        rev,
        UnexpectedState,
        RevisionNotFound(_, _)
      )
      .map(_.toResource(minimum))
      .surround("fetchPermissionsAt")

  override def replace(
      permissions: Set[Permission],
      rev: Int
  )(using caller: Subject): IO[PermissionsResource] =
    eval(ReplacePermissions(rev, permissions, caller)).surround("replacePermissions")

  override def append(
      permissions: Set[Permission],
      rev: Int
  )(using caller: Subject): IO[PermissionsResource] =
    eval(AppendPermissions(rev, permissions, caller)).surround("appendPermissions")

  override def subtract(
      permissions: Set[Permission],
      rev: Int
  )(using caller: Subject): IO[PermissionsResource] =
    eval(SubtractPermissions(rev, permissions, caller)).surround("subtractPermissions")

  override def delete(rev: Int)(using caller: Subject): IO[PermissionsResource] =
    eval(DeletePermissions(rev, caller)).surround("deletePermissions")

  private def eval(cmd: PermissionsCommand): IO[PermissionsResource] =
    log
      .evaluate(labelId, cmd)
      .map { case (_, state) =>
        state.toResource(minimum)
      }
}

object PermissionsImpl {

  type PermissionsLog =
    GlobalEventLog[Label, PermissionsState, PermissionsCommand, PermissionsEvent, PermissionsRejection]

  /**
    * Constructs a new [[Permissions]] instance
    * @param config
    *   the permissions module configuration
    * @param xas
    *   the doobie transactors
    */
  final def apply(
      config: PermissionsConfig,
      xas: Transactors,
      clock: Clock[IO]
  )(using Tracer[IO]): Permissions =
    new PermissionsImpl(
      config.minimum,
      GlobalEventLog(Permissions.definition(config.minimum, clock), config.eventLog, xas)
    )
}
