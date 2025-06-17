package ai.senscience.nexus.delta.sourcing.implicits

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import doobie.{Get, Put}

trait IriInstances {

  implicit final val iriGet: Get[Iri] = Get[String].temap(Iri(_))
  implicit final val iriPut: Put[Iri] = Put[String].contramap(_.toString)

}

object IriInstances extends IriInstances
