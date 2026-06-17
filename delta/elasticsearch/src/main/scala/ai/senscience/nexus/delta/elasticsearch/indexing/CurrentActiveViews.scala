package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.SuccessElemStream
import cats.effect.IO
import fs2.Stream

trait CurrentActiveViews {

  def stream(project: ProjectRef): Stream[IO, ActiveViewDef]

  def stream: Stream[IO, ActiveViewDef]

  /**
    * The current active view for the given project and id, if any (deprecated/unknown views yield `None`).
    */
  def fetch(project: ProjectRef, id: Iri): IO[Option[ActiveViewDef]]
}

object CurrentActiveViews {

  def apply(views: ElasticSearchViews): CurrentActiveViews = new CurrentActiveViews {

    override def stream(project: ProjectRef): Stream[IO, ActiveViewDef] =
      keepActive(views.currentIndexingViews(project))

    override def stream: Stream[IO, ActiveViewDef] =
      keepActive(views.currentIndexingViews)

    override def fetch(project: ProjectRef, id: Iri): IO[Option[ActiveViewDef]] =
      views.fetchIndexingView(id, project).attempt.map(_.toOption)

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

    override def fetch(project: ProjectRef, id: Iri): IO[Option[ActiveViewDef]] =
      IO.pure(views.find(v => v.ref.project == project && v.ref.viewId == id))
  }

}
