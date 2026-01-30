package ai.senscience.nexus.delta.rdf.instances

import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Triple.*
import org.apache.jena.graph.Node
import org.http4s.Uri

import java.time.Instant

trait TripleInstances {
  // $COVERAGE-OFF$
  given createSubjectFromIriOrBNode: Conversion[IriOrBNode, Node] = subject(_)
  given createPredicateFromIri: Conversion[Iri, Node]             = predicate(_)
  given createObjectFromString: Conversion[String, Node]          = obj(_)
  given createObjectFromInt: Conversion[Int, Node]                = obj(_)
  given createObjectFromLong: Conversion[Long, Node]              = obj(_)
  given createObjectFromBoolean: Conversion[Boolean, Node]        = obj(_)
  given createObjectFromDouble: Conversion[Double, Node]          = obj(_)
  given createObjectFromIri: Conversion[IriOrBNode, Node]         = obj(_)
  given createObjectFromUri: Conversion[Uri, Node]                = obj(_)
  given createObjectFromInstant: Conversion[Instant, Node]        = obj(_)
  // $COVERAGE-ON$
}
