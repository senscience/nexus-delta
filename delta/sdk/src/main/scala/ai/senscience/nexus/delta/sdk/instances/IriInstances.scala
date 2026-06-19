package ai.senscience.nexus.delta.sdk.instances

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import org.typelevel.doobie.{Get, Put}

trait IriInstances {

  given Get[Iri] = Get[String].temap(Iri(_))
  given Put[Iri] = Put[String].contramap(_.toString)

}
