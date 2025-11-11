package ai.senscience.nexus.delta.sdk.organizations

import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.{OrganizationIsDeprecated, OrganizationNotFound}
import ai.senscience.nexus.delta.sdk.organizations.model.{Organization, OrganizationState}
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.state.GlobalStateGet
import cats.effect.IO
import doobie.syntax.all.*
import doobie.{Get, Put}

trait FetchActiveOrganization {

  def apply(org: Label): IO[Organization]

}

object FetchActiveOrganization {

  private given Put[Label]             = OrganizationState.serializer.putId
  private given Get[OrganizationState] = OrganizationState.serializer.getValue

  def apply(xas: Transactors): FetchActiveOrganization = (org: Label) =>
    GlobalStateGet[Label, OrganizationState](Organizations.entityType, org)
      .transact(xas.write)
      .flatMap {
        case None                    => IO.raiseError(OrganizationNotFound(org))
        case Some(o) if o.deprecated => IO.raiseError(OrganizationIsDeprecated(org))
        case Some(o)                 => IO.pure(o.toResource.value)
      }

}
