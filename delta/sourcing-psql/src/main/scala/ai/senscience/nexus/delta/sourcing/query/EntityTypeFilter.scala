package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.model.EntityType
import ai.senscience.nexus.delta.sourcing.FragmentEncoder
import cats.data.NonEmptyList
import doobie.Fragments
import doobie.syntax.all.*

sealed trait EntityTypeFilter

object EntityTypeFilter {

  case object All extends EntityTypeFilter

  final case class Include(types: NonEmptyList[EntityType]) extends EntityTypeFilter

  def include(head: EntityType, tail: EntityType*) = Include(NonEmptyList.of(head, tail*))

  def include(set: Set[EntityType]): EntityTypeFilter = Include(NonEmptyList.fromListUnsafe(set.toList))

  given FragmentEncoder[EntityTypeFilter] = FragmentEncoder.instance {
    case All            => None
    case Include(types) => Some(Fragments.in(fr"type", types))
  }

}
