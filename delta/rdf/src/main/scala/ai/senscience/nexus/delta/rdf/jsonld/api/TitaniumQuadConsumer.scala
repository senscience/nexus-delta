package ai.senscience.nexus.delta.rdf.jsonld.api

import com.apicatalog.rdf.api.RdfQuadConsumer
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.system.{ParserProfile, StreamRDF}
import org.apache.jena.sparql.core.Quad

/**
  * Feeds the quads produced by Titanium into a Jena [[StreamRDF]].
  *
  * Jena ships an equivalent bridge as `TitaniumToJena.JsonLDToStreamRDF`, but it is package-private and only reachable
  * through `TitaniumToJena.convert`, which always expands its input first. Delta hands already-expanded documents to
  * Titanium, so it drives `ToRdfProcessor` directly and needs its own consumer.
  */
final private[api] class TitaniumQuadConsumer(output: StreamRDF, profile: ParserProfile) extends RdfQuadConsumer {

  import TitaniumQuadConsumer.{col, line}

  override def quad(
      subject: String,
      predicate: String,
      obj: String,
      datatype: String,
      language: String,
      direction: String,
      graph: String
  ): RdfQuadConsumer = {
    val s = node(subject)
    val p = node(predicate)
    val o =
      if RdfQuadConsumer.isLiteral(datatype, language, direction) then literal(obj, datatype, language, direction)
      else node(obj)

    if graph == null then output.triple(Triple.create(s, p, o))
    else output.quad(Quad.create(node(graph), s, p, o))
    this
  }

  private def node(value: String): Node =
    if RdfQuadConsumer.isBlank(value) then profile.getFactorRDF.createBlankNode(value.substring(2))
    else profile.createURI(profile.resolveIRI(value, line, col), line, col)

  private def literal(lexical: String, datatype: String, language: String, direction: String): Node =
    if RdfQuadConsumer.isLangString(datatype, language, direction) then
      profile.createLangLiteral(lexical, language, line, col)
    else if RdfQuadConsumer.isDirLangString(datatype, language, direction) then
      profile.createLangDirLiteral(lexical, language, direction, line, col)
    else profile.createTypedLiteral(lexical, TypeMapper.getInstance().getSafeTypeByName(datatype), line, col)
}

private[api] object TitaniumQuadConsumer {
  // Titanium does not track positions in the source document
  private val line = -1L
  private val col  = -1L
}
