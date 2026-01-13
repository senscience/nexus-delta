package org.apache.jena.riot.lang

import com.apicatalog.jsonld.serialization.QuadsToJsonld
import com.apicatalog.rdf.api.RdfQuadConsumer
import org.apache.jena.graph.Node
import org.apache.jena.riot.system.{ParserProfile, StreamRDF}
import org.apache.jena.sparql.core.{DatasetGraph, Quad}

// TODO: Remove when Jena allows to convert to/from Json-LD outside of writing files
object TitaniumConverter {
  def apply(output: StreamRDF, profile: ParserProfile): RdfQuadConsumer =
    new LangJSONLD11.JsonLDToStreamRDF(output, profile)

  def transform(dsg: DatasetGraph, consumer: QuadsToJsonld): Unit = {
    dsg.stream().forEach { quad =>
      val s   = resource(quad.getSubject)
      val p   = resource(quad.getPredicate)
      val g   = resourceGraphName(quad.getGraph)
      val obj = quad.getObject

      if obj.isURI || obj.isBlank then {
        val o = resource(obj)
        consumer.quad(s, p, o, null, null, null, g)
        ()
      } else if obj.isLiteral then {
        val lex      = obj.getLiteralLexicalForm
        val datatype = obj.getLiteralDatatypeURI
        val lang     = Option(obj.getLiteralLanguage).filter(_.nonEmpty).orNull
        val dir      = Option(obj.getLiteralBaseDirection).map(_.toString).orNull
        consumer.quad(s, p, lex, datatype, lang, dir, g)
        ()
      } else if obj.isTripleTerm then throw new IllegalStateException("Triple terms not supported for JSON-LD")
      else throw new IllegalStateException(s"Encountered unexpected term: $obj")

    }
  }

  private def resourceGraphName(gn: Node): String =
    Option(gn).filterNot(Quad.isDefaultGraph).map(resource).orNull

  private def resource(term: Node): String =
    Option
      .when(term.isBlank)(s"_:${term.getBlankNodeLabel}")
      .orElse(Option.when(term.isURI)(term.getURI))
      .getOrElse(throw new IllegalArgumentException("Not a URI or a blank node"))
}
