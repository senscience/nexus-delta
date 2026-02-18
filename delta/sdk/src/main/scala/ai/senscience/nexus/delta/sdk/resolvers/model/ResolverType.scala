package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.{Decoder, Encoder, Json}

/**
  * Enumeration of resolver types
  */
enum ResolverType(tpe: Iri) {
  def types: Set[Iri] = Set(nxv.Resolver, tpe)

  /**
    * Resolver within a same project
    */
  case InProject extends ResolverType(nxv.InProject)

  /**
    * Resolver across multiple projects
    */
  case CrossProject extends ResolverType(nxv.CrossProject)
}

object ResolverType {

  given Encoder[ResolverType] = Encoder.instance {
    case InProject    => Json.fromString("InProject")
    case CrossProject => Json.fromString("CrossProject")
  }

  given Decoder[ResolverType] = Decoder.decodeString.emap {
    case "InProject"    => Right(InProject)
    case "CrossProject" => Right(CrossProject)
  }

}
