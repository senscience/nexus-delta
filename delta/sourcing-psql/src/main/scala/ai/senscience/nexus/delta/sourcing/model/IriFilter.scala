package ai.senscience.nexus.delta.sourcing.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.sourcing.model.IriFilter.Include
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}

sealed trait IriFilter {
  def asRestrictedTo: Option[Include] = this match {
    case IriFilter.None => None
    case r: Include     => Some(r)
  }
}

object IriFilter {
  case object None                                   extends IriFilter
  sealed abstract case class Include(iris: Set[Iri]) extends IriFilter

  def fromSet(iris: Set[Iri]): IriFilter = if iris.nonEmpty then new Include(iris) {} else None
  def restrictedTo(iri: Iri): IriFilter  = fromSet(Set(iri))

  given Encoder[IriFilter]       = {
    case None          => Json.arr()
    case Include(iris) => iris.asJson
  }
  given Decoder[IriFilter]       = Decoder[Set[Iri]].map(fromSet)
  given JsonLdDecoder[IriFilter] = JsonLdDecoder[Set[Iri]].map(fromSet)
}
