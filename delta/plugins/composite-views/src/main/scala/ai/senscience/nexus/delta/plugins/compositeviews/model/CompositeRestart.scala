package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

/**
  * Possible restart strategies for a composite view
  */
sealed trait CompositeRestart extends Product with Serializable {

  /**
    * The reference to the view
    */
  def view: ViewRef

  /**
    * Id of the composite view
    */
  def id: Iri = view.viewId

  /**
    * The project of the composite view
    */
  def project: ProjectRef = view.project

  /**
    * the instant the user performed the action
    */
  def instant: Instant

  /**
    * the user who performed the action
    */
  def subject: Subject
}

object CompositeRestart {

  val entityType: EntityType = EntityType("composite-restart")

  /**
    * Restarts the view indexing process. It does not delete the created indices/namespaces but it overrides the
    * graphs/documents when going through the log.
    */
  final case class FullRestart(view: ViewRef, instant: Instant, subject: Subject) extends CompositeRestart

  /**
    * Restarts indexing process for all targets while keeping the sources (and the intermediate Sparql space) progress
    */
  final case class FullRebuild(view: ViewRef, instant: Instant, subject: Subject) extends CompositeRestart

  object FullRebuild {

    /**
      * Generates a full rebuild event
      */
    def auto(viewRef: ViewRef): FullRebuild = FullRebuild(viewRef, Instant.EPOCH, Anonymous)
  }

  /**
    * Restarts indexing process for the provided target while keeping the sources (and the intermediate Sparql space)
    * progress
    * @param target
    *   the projection to restart
    */
  final case class PartialRebuild(view: ViewRef, target: Iri, instant: Instant, subject: Subject)
      extends CompositeRestart

  object PartialRebuild {

    /**
      * Generates a partial rebuild event
      */
    def auto(viewRef: ViewRef, target: Iri): PartialRebuild = PartialRebuild(viewRef, target, Instant.EPOCH, Anonymous)
  }

  implicit val compositeRestartCodec: Codec.AsObject[CompositeRestart] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration = Serializer.circeConfiguration
    deriveConfiguredCodec[CompositeRestart]
  }

}
