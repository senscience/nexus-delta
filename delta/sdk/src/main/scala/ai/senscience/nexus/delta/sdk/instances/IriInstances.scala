package ai.senscience.nexus.delta.sdk.instances

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import doobie.{Get, Put}

trait IriInstances {

  given Get[Iri] = Get[String].temap(Iri(_))
  given Put[Iri] = Put[String].contramap(_.toString)

}
