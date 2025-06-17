package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import cats.effect.IO

/**
  * Describes the SPARQL [[ServiceDependency]] providing a way to extract the [[ServiceDescription]]
  */
class SparqlServiceDependency(client: SparqlClient) extends ServiceDependency {

  override def serviceDescription: IO[ServiceDescription] = client.serviceDescription
}
