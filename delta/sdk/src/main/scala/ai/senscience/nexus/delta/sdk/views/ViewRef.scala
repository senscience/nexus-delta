package ai.senscience.nexus.delta.sdk.views

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.semiauto.deriveDefaultJsonLdDecoder
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.Order
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

/**
  * View reference.
  *
  * @param project
  *   the project to which the view belongs
  * @param viewId
  *   the view id
  */
final case class ViewRef(project: ProjectRef, viewId: Iri) {
  override def toString: String = s"$project/$viewId"
}

object ViewRef {

  def unsafe(org: String, project: String, viewId: Iri): ViewRef =
    ViewRef(ProjectRef.unsafe(org, project), viewId)

  given Codec.AsObject[ViewRef] = deriveCodec[ViewRef]

  given JsonLdDecoder[ViewRef] = deriveDefaultJsonLdDecoder[ViewRef]

  given Order[ViewRef] = Order.by { viewRef =>
    (viewRef.viewId, viewRef.project)
  }

}
