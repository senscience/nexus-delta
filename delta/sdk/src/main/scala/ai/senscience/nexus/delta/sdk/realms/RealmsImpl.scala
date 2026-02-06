package ai.senscience.nexus.delta.sdk.realms

import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.sdk.RealmResource
import ai.senscience.nexus.delta.sdk.model.search.{SearchParams, SearchResults}
import ai.senscience.nexus.delta.sdk.realms.RealmsImpl.RealmsLog
import ai.senscience.nexus.delta.sdk.realms.model.*
import ai.senscience.nexus.delta.sdk.realms.model.RealmCommand.{CreateRealm, DeprecateRealm, UpdateRealm}
import ai.senscience.nexus.delta.sdk.realms.model.RealmRejection.{RealmNotFound, RevisionNotFound}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.{GlobalEventLog, Transactors}
import cats.effect.{Clock, IO}
import org.typelevel.otel4s.trace.Tracer

final class RealmsImpl private (log: RealmsLog)(using Tracer[IO]) extends Realms {

  override def create(
      label: Label,
      fields: RealmFields
  )(using caller: Subject): IO[RealmResource] = {
    val command = CreateRealm(label, fields.name, fields.openIdConfig, fields.logo, fields.acceptedAudiences, caller)
    eval(command).surround("createRealm")
  }

  override def update(
      label: Label,
      rev: Int,
      fields: RealmFields
  )(using caller: Subject): IO[RealmResource] = {
    val command =
      UpdateRealm(label, rev, fields.name, fields.openIdConfig, fields.logo, fields.acceptedAudiences, caller)
    eval(command).surround("updateRealm")
  }

  override def deprecate(label: Label, rev: Int)(using caller: Subject): IO[RealmResource] =
    eval(DeprecateRealm(label, rev, caller)).surround("deprecateRealm")

  private def eval(cmd: RealmCommand): IO[RealmResource] =
    log.evaluate(cmd.label, cmd).map(_.state.toResource)

  override def fetch(label: Label): IO[RealmResource] =
    log
      .stateOr(label, RealmNotFound(label))
      .map(_.toResource)
      .surround("fetchRealm")

  override def fetchAt(label: Label, rev: Int): IO[RealmResource] =
    log
      .stateOr(label, rev, RealmNotFound(label), RevisionNotFound(_, _))
      .map(_.toResource)
      .surround("fetchRealmAt")

  override def list(
      pagination: Pagination.FromPagination,
      params: SearchParams.RealmSearchParams,
      ordering: Ordering[RealmResource]
  ): IO[SearchResults.UnscoredSearchResults[RealmResource]] =
    SearchResults(
      log.currentStates(_.toResource).evalFilter(params.matches),
      pagination,
      ordering
    ).surround("listRealms")
}

object RealmsImpl {

  type RealmsLog = GlobalEventLog[Label, RealmState, RealmCommand, RealmEvent, RealmRejection]

  final def apply(
      config: RealmsConfig,
      wellKnownResolver: WellKnownResolver,
      xas: Transactors,
      clock: Clock[IO]
  )(using Tracer[IO]): Realms = new RealmsImpl(
    GlobalEventLog(Realms.definition(wellKnownResolver, OpenIdExists(xas), clock), config.eventLog, xas)
  )

}
