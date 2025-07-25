package ai.senscience.nexus.delta.plugins.compositeviews.stream

import ai.senscience.nexus.delta.plugins.compositeviews.indexing.MetadataPredicates
import ai.senscience.nexus.delta.rdf.RdfError.MissingPredicate
import ai.senscience.nexus.delta.rdf.Triple
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.NQuads
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.time.Instant

class RemoteGraphStreamSuite extends NexusSuite {

  private val metadataPredicates = MetadataPredicates(
    Set(
      nxv.self.iri,
      nxv.updatedBy.iri,
      nxv.updatedAt.iri,
      nxv.createdBy.iri,
      nxv.createdAt.iri,
      nxv.constrainedBy.iri,
      nxv.rev.iri,
      nxv.deprecated.iri,
      nxv.project.iri
    ).map(Triple.predicate)
  )

  private val id = iri"https://example.com/testresource"

  private val project = ProjectRef.unsafe("org", "proj")
  private val elem    = SuccessElem(EntityType("test"), id, project, Instant.EPOCH, Offset.Start, (), 1)
  private val nQuads  = NQuads(contentOf("remote/resource.nq"), id)

  test("Metadata should be filtered correctly") {
    for {
      resource <- RemoteGraphStream.fromNQuads(elem, project, nQuads, metadataPredicates)
      _         = assertEquals(resource.id, id)
      _         = assertEquals(resource.deprecated, false)
      _         = assertEquals(resource.schema, Latest(iri"https://bluebrain.github.io/nexus/schemas/unconstrained.json"))
      _         = assertEquals(resource.types, Set(iri"https://example.com/Type1"))
      _         = assertEquals(resource.graph.triples.size, 2)
      _         = assertEquals(resource.metadataGraph.triples.size, 9)
    } yield ()
  }

  test("Fail when schema predicate is missing") {
    val nQuadsNoSchema = NQuads(contentOf("remote/resource-no-schema.nq"), id)
    RemoteGraphStream
      .fromNQuads(elem, project, nQuadsNoSchema, metadataPredicates)
      .interceptEquals(MissingPredicate(nxv.constrainedBy.iri))
  }

  test("Fail when deprecated predicate is missing") {
    val nQuadsNoDeprecated = NQuads(contentOf("remote/resource-no-deprecated.nq"), id)
    RemoteGraphStream
      .fromNQuads(elem, project, nQuadsNoDeprecated, metadataPredicates)
      .interceptEquals(MissingPredicate(nxv.deprecated.iri))
  }

}
