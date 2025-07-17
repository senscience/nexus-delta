package ai.senscience.nexus.delta.sdk.views

import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.model.search.{ResultEntry, SearchResults}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import cats.syntax.all.*

trait ViewsList {

  def apply(project: ProjectRef): IO[SearchResults[ResourceF[Unit]]]

}

object ViewsList {

  private val empty = SearchResults.UnscoredSearchResults(0L, Seq.empty[ResultEntry[ResourceF[Unit]]])

  final class AggregateViewsList(val internal: List[ViewsList]) extends ViewsList {
    override def apply(project: ProjectRef): IO[SearchResults[ResourceF[Unit]]] =
      internal.foldLeftM(empty) { case (acc, vl) =>
        vl(project).map { current =>
          SearchResults.UnscoredSearchResults(acc.total + current.total, acc.results ++ current.results)
        }
      }
  }

  def apply[A](f: ProjectRef => IO[SearchResults[ResourceF[A]]]): ViewsList = new ViewsList {
    override def apply(project: ProjectRef): IO[SearchResults[ResourceF[Unit]]] =
      f(project).map(_.map(_.void))
  }
}
