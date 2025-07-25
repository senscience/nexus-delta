package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.sparql.core.Quad
import org.http4s.Uri

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.Locale

// $COVERAGE-OFF$
object Triple {

  /**
    * An RDF triple. Subject must be an Iri or a blank node. Predicate must be an Iri. Object must be an Iri, a blank
    * node or a Literal.
    */
  type Triple = (Node, Node, Node)

  private val eFormatter = new DecimalFormat("0.###############E0", new DecimalFormatSymbols(Locale.ENGLISH))
  eFormatter.setMinimumFractionDigits(1)

  def subject(value: IriOrBNode): Node =
    value match {
      case Iri(iri)             => NodeFactory.createURI(iri.toString)
      case IriOrBNode.BNode(id) => NodeFactory.createBlankNode(id)
    }

  def bNode: Node =
    NodeFactory.createBlankNode()

  def predicate(value: Iri): Node =
    NodeFactory.createURI(value.toString)

  def obj(value: Uri): Node =
    NodeFactory.createURI(value.toString)

  def obj(value: String, lang: Option[String] = None): Node =
    lang.fold(NodeFactory.createLiteralString(value))(l => NodeFactory.createLiteralLang(value, l))

  def obj(value: String, dataType: Option[Iri], languageTag: Option[String]): Node =
    dataType match {
      case Some(dt) =>
        val tpe = TypeMapper.getInstance().getSafeTypeByName(dt.toString)
        if (tpe.isValid(value)) NodeFactory.createLiteralDT(value, tpe)
        else obj(value, languageTag)
      case None     => obj(value, languageTag)

    }

  def obj(value: Boolean): Node =
    NodeFactory.createLiteralDT(value.toString, XSDDatatype.XSDboolean)

  def obj(value: Int): Node =
    NodeFactory.createLiteralDT(value.toString, XSDDatatype.XSDinteger)

  def obj(value: Long): Node =
    NodeFactory.createLiteralDT(value.toString, XSDDatatype.XSDinteger)

  def obj(value: Double): Node =
    NodeFactory.createLiteralDT(eFormatter.format(value), XSDDatatype.XSDdouble)

  def obj(value: Float): Node =
    obj(value.toDouble)

  def obj(value: Instant): Node =
    NodeFactory.createLiteralDT(
      value.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT),
      XSDDatatype.XSDdateTime
    )

  def obj(value: IriOrBNode): Node =
    subject(value)

  final def apply(quad: Quad): Triple =
    (quad.getSubject, quad.getPredicate, quad.getObject)

}
// $COVERAGE-ON$
