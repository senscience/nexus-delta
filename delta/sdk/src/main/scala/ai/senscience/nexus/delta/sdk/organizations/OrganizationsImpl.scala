package ai.senscience.nexus.delta.sdk.organizations

import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.sdk.model.search.{SearchParams, SearchResults}
import ai.senscience.nexus.delta.sdk.organizations.OrganizationsImpl.OrganizationsLog
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationCommand.*
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.*
import ai.senscience.nexus.delta.sdk.organizations.model.{OrganizationCommand, OrganizationEvent, OrganizationRejection, OrganizationState}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.{OrganizationResource, ScopeInitializer}
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.{GlobalEventLog, Transactors}
import cats.effect.{Clock, IO}
import org.typelevel.otel4s.trace.Tracer

final class OrganizationsImpl private (
    log: OrganizationsLog,
    scopeInitializer: ScopeInitializer
)(using Tracer[IO])
    extends Organizations {

  override def create(
      label: Label,
      description: Option[String]
  )(using caller: Subject): IO[OrganizationResource] =
    for {
      resource <- eval(CreateOrganization(label, description, caller)).surround("createOrganization")
      _        <- scopeInitializer
                    .initializeOrganization(resource)
                    .surround("initializeOrganization")
    } yield resource

  override def update(
      label: Label,
      description: Option[String],
      rev: Int
  )(using caller: Subject): IO[OrganizationResource] =
    eval(UpdateOrganization(label, rev, description, caller)).surround("updateOrganization")

  override def deprecate(
      label: Label,
      rev: Int
  )(using caller: Subject): IO[OrganizationResource] =
    eval(DeprecateOrganization(label, rev, caller)).surround("deprecateOrganization")

  override def undeprecate(org: Label, rev: Int)(using caller: Subject): IO[OrganizationResource] = {
    eval(UndeprecateOrganization(org, rev, caller)).surround("undeprecateOrganization")
  }

  override def fetch(label: Label): IO[OrganizationResource] =
    log.stateOr(label, OrganizationNotFound(label)).map(_.toResource).surround("fetchOrganization")

  override def fetchAt(label: Label, rev: Int): IO[OrganizationResource] =
    log
      .stateOr(label, rev, OrganizationNotFound(label), RevisionNotFound(_, _))
      .map(_.toResource)
      .surround("fetchOrganizationAt")

  private def eval(cmd: OrganizationCommand): IO[OrganizationResource] =
    log.evaluate(cmd.label, cmd).map(_.state.toResource)

  override def list(
      pagination: Pagination.FromPagination,
      params: SearchParams.OrganizationSearchParams,
      ordering: Ordering[OrganizationResource]
  ): IO[SearchResults.UnscoredSearchResults[OrganizationResource]] =
    SearchResults(
      log
        .currentStates(_.toResource)
        .evalFilter(params.matches),
      pagination,
      ordering
    ).surround("listOrganizations")

  override def purge(org: Label): IO[Unit] = log.delete(org)
}

object OrganizationsImpl {

  type OrganizationsLog =
    GlobalEventLog[Label, OrganizationState, OrganizationCommand, OrganizationEvent, OrganizationRejection]

  def apply(
      scopeInitializer: ScopeInitializer,
      config: EventLogConfig,
      xas: Transactors,
      clock: Clock[IO]
  )(using UUIDF, Tracer[IO]): Organizations =
    new OrganizationsImpl(
      GlobalEventLog(Organizations.definition(clock), config, xas),
      scopeInitializer
    )

}
