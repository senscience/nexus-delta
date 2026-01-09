package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.rdf.Triple
import ai.senscience.nexus.delta.rdf.jsonld.context.{JsonLdContext, RemoteContextResolution}
import ai.senscience.nexus.delta.sdk.model.MetadataContextValue
import cats.effect.IO
import org.apache.jena.graph.Node

/**
  * Collection of metadata predicates.
  */
final case class MetadataPredicates(values: Set[Node])

object MetadataPredicates {
  def apply(searchMetadata: MetadataContextValue): IO[MetadataPredicates] =
    JsonLdContext(searchMetadata.value)(using RemoteContextResolution.never)
      .map(_.aliasesInv.keySet.map(Triple.predicate))
      .map(new MetadataPredicates(_))
}
