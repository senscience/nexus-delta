package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import cats.effect.IO

/**
  * Describes the Elasticsearch [[ServiceDependency]] providing a way to extract the [[ServiceDescription]] from
  * Elasticsearch
  */
class ElasticSearchServiceDependency(client: ElasticSearchClient) extends ServiceDependency {

  override def serviceDescription: IO[ServiceDescription] =
    client.serviceDescription
}
