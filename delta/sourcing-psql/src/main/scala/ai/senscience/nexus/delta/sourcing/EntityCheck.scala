package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.sourcing.model.Tag.Latest
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label, ProjectRef, Tag}
import cats.data.NonEmptySet
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.syntax.all.*

object EntityCheck {

  /**
    * Allows to find the [[EntityType]] of the resource with the given [[Id]] in the given project
    */
  def findType[Id](id: Id, project: ProjectRef, xas: Transactors)(implicit putId: Put[Id]): IO[Option[EntityType]] =
    sql"""
         | SELECT type
         | FROM public.scoped_states
         | WHERE org = ${project.organization}
         | AND project = ${project.project}
         | AND id = $id
         | AND tag = ${Tag.latest}
         |""".stripMargin.query[EntityType].option.transact(xas.read)

  /**
    * Raises the defined error if at least one of the provided references does not exist or is deprecated
    * @param tpe
    *   the type of the different resources
    * @param refs
    *   the references to test
    * @param onUnknownOrDeprecated
    *   the error to raise on the failiing references
    */
  def raiseMissingOrDeprecated[Id, E <: Throwable](
      tpe: EntityType,
      refs: NonEmptySet[(ProjectRef, Id)],
      onUnknownOrDeprecated: Set[(ProjectRef, Id)] => E,
      xas: Transactors
  )(implicit getId: Get[Id], putId: Put[Id]): IO[Unit] = {
    val fragments    = refs.toNonEmptyList.map { case (p, id) =>
      fr"org = ${p.organization} AND project = ${p.project}  AND id = $id AND tag = ${Latest.value} AND deprecated = false"
    }
    val or: Fragment = Fragments.or(fragments)

    sql"""SELECT org, project, id FROM public.scoped_states WHERE type = $tpe and $or"""
      .query[(Label, Label, Id)]
      .map { case (org, proj, id) =>
        ProjectRef(org, proj) -> id
      }
      .to[Set]
      .transact(xas.read)
      .flatMap { result =>
        IO.raiseWhen(result.size < refs.size) {
          onUnknownOrDeprecated(refs.toList.toSet.diff(result))
        }
      }
  }
}
