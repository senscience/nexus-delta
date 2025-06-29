package ai.senscience.nexus.delta.sdk.permissions

import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.sdk.PermissionsResource
import ai.senscience.nexus.delta.sdk.permissions.Permissions.{entityType, labelId}
import ai.senscience.nexus.delta.sdk.permissions.PermissionsImpl.PermissionsLog
import ai.senscience.nexus.delta.sdk.permissions.model.*
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsCommand.*
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsRejection.{RevisionNotFound, UnexpectedState}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.{GlobalEventLog, Transactors}
import cats.effect.{Clock, IO}

final class PermissionsImpl private (
    override val minimum: Set[Permission],
    log: PermissionsLog
) extends Permissions {

  implicit private val kamonComponent: KamonMetricComponent = KamonMetricComponent(entityType.value)

  private val initial = PermissionsState.initial(minimum)

  override def fetch: IO[PermissionsResource] =
    log
      .stateOr[PermissionsRejection](labelId, UnexpectedState)
      .handleErrorWith(_ => IO.pure(initial))
      .map(_.toResource(minimum))
      .span("fetchPermissions")

  override def fetchAt(rev: Int): IO[PermissionsResource] =
    log
      .stateOr(
        labelId,
        rev,
        UnexpectedState,
        RevisionNotFound
      )
      .map(_.toResource(minimum))
      .span("fetchPermissionsAt")

  override def replace(
      permissions: Set[Permission],
      rev: Int
  )(implicit caller: Subject): IO[PermissionsResource] =
    eval(ReplacePermissions(rev, permissions, caller)).span("replacePermissions")

  override def append(
      permissions: Set[Permission],
      rev: Int
  )(implicit caller: Subject): IO[PermissionsResource] =
    eval(AppendPermissions(rev, permissions, caller)).span("appendPermissions")

  override def subtract(
      permissions: Set[Permission],
      rev: Int
  )(implicit caller: Subject): IO[PermissionsResource] =
    eval(SubtractPermissions(rev, permissions, caller)).span("subtractPermissions")

  override def delete(rev: Int)(implicit caller: Subject): IO[PermissionsResource] =
    eval(DeletePermissions(rev, caller)).span("deletePermissions")

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
  ): Permissions =
    new PermissionsImpl(
      config.minimum,
      GlobalEventLog(Permissions.definition(config.minimum, clock), config.eventLog, xas)
    )
}
