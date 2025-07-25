package ai.senscience.nexus.delta.sdk.views

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sdk.views.View.{AggregateView, IndexingView}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.{EntityDependencyStore, Serializer, Transactors}
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Decoder

trait ViewsStore[Rejection] {

  /**
    * Fetch the view with the given id in the given project and maps it to a view
    * @param id
    *   the view identifier
    * @param project
    *   the view
    * @return
    */
  def fetch(id: IdSegmentRef, project: ProjectRef): IO[View]

}

object ViewsStore {

  private val logger = Logger[ViewsStore.type]

  import ai.senscience.nexus.delta.sourcing.implicits.*

  def apply[Rejection, Value](
      serializer: Serializer[Iri, Value],
      fetchValue: (IdSegmentRef, ProjectRef) => IO[Value],
      asView: Value => IO[Either[Iri, IndexingView]],
      xas: Transactors
  ): ViewsStore[Rejection] = new ViewsStore[Rejection] {

    implicit val stateDecoder: Decoder[Value] = serializer.codec

    // For embedded views in aggregate view drop intermediate aggregate view and those who raise an error
    private def embeddedView(project: ProjectRef, id: Iri, value: Value): IO[Option[IndexingView]] =
      asView(value).redeemWith(
        rejection => logger.debug(s"View '$id' in project '$project' is skipped because of '$rejection'.") >> IO.none,
        v =>
          logger.debug(
            s"View '$id' in project '$project' is skipped because it is an intermediate aggregate view."
          ) >> IO.pure(v.toOption)
      )

    override def fetch(id: IdSegmentRef, project: ProjectRef): IO[View] =
      for {
        res              <- fetchValue(id, project).flatMap(asView)
        singleOrMultiple <- res match {
                              case Left(iri)   =>
                                EntityDependencyStore
                                  .decodeRecursiveDependencies[Iri, Value](project, iri, xas)
                                  .flatMap {
                                    _.traverseFilter(embeddedView(project, iri, _)).map(AggregateView(_))
                                  }
                              case Right(view) => IO.pure(view)
                            }
      } yield singleOrMultiple

  }
}
