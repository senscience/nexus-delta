package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.{InvalidViewReferences, PermissionIsNotDefined, TooManyViewReferences}
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.{AggregateBlazegraphViewValue, IndexingBlazegraphViewValue}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.sdk.permissions.model.Permission
import ch.epfl.bluebrain.nexus.delta.sdk.views.ValidateAggregate
import ch.epfl.bluebrain.nexus.delta.sourcing.Transactors

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
        InvalidViewReferences,
        maxViewRefs,
        TooManyViewReferences,
        xas
      )(v.views)
    case v: IndexingBlazegraphViewValue  =>
      fetchPermissions.flatMap { perms =>
        IO.whenA(!perms.contains(v.permission))(IO.raiseError(PermissionIsNotDefined(v.permission)))
      }
  }

}
