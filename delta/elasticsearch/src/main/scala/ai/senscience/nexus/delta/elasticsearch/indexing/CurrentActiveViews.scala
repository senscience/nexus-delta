package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.SuccessElemStream
import cats.effect.IO
import fs2.Stream

trait CurrentActiveViews {

  def stream(project: ProjectRef): Stream[IO, ActiveViewDef]

  def stream: Stream[IO, ActiveViewDef]
}

object CurrentActiveViews {

  def apply(views: ElasticSearchViews): CurrentActiveViews = new CurrentActiveViews {

    override def stream(project: ProjectRef): Stream[IO, ActiveViewDef] =
      keepActive(views.currentIndexingViews(project))

    override def stream: Stream[IO, ActiveViewDef] =
      keepActive(views.currentIndexingViews)

    private def keepActive(stream: SuccessElemStream[IndexingViewDef]) =
      stream.evalMapFilter {
        _.value match {
          case a: ActiveViewDef => IO.some(a)
          case _                => IO.none
        }
      }
  }

  def apply(views: ActiveViewDef*): CurrentActiveViews = new CurrentActiveViews {
    override def stream(project: ProjectRef): Stream[IO, ActiveViewDef] = stream.filter(_.ref.project == project)

    override def stream: Stream[IO, ActiveViewDef] = Stream.iterable(views)
  }

}
