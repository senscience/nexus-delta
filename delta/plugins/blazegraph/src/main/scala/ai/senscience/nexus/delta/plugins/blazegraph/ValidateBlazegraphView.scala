package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.{InvalidViewReferences, PermissionIsNotDefined, TooManyViewReferences}
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.{AggregateBlazegraphViewValue, IndexingBlazegraphViewValue}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.ValidateAggregate
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.IO

/**
  * Validate an [[BlazegraphViewValue]] during command evaluation
  */
trait ValidateBlazegraphView {

  def apply(value: BlazegraphViewValue): IO[Unit]
}

object ValidateBlazegraphView {

  def apply(fetchPermissions: IO[Set[Permission]], maxViewRefs: Int, xas: Transactors): ValidateBlazegraphView = {
    case v: AggregateBlazegraphViewValue =>
      ValidateAggregate(
        BlazegraphViews.entityType,
        InvalidViewReferences(_),
        maxViewRefs,
        TooManyViewReferences(_, _),
        xas
      )(v.views)
    case v: IndexingBlazegraphViewValue  =>
      fetchPermissions.flatMap { perms =>
        IO.whenA(!perms.contains(v.permission))(IO.raiseError(PermissionIsNotDefined(v.permission)))
      }
  }

}
