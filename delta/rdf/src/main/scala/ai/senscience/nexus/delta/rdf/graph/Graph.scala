package ai.senscience.nexus.delta.rdf.graph

import ai.senscience.nexus.delta.rdf.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.Quad.Quad
import ai.senscience.nexus.delta.rdf.RdfError.{ParsingError, SparqlConstructQueryError, UnexpectedJsonLd}
import ai.senscience.nexus.delta.rdf.Triple.{predicate, subject, Triple}
import ai.senscience.nexus.delta.rdf.Vocabulary.rdf
import ai.senscience.nexus.delta.rdf.graph.Graph.fakeId
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jena.writer.DotWriter.*
import ai.senscience.nexus.delta.rdf.jsonld.api.TitaniumJsonLdApi.*
import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdApi
import ai.senscience.nexus.delta.rdf.jsonld.context.*
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import org.apache.jena.graph.Node
import org.apache.jena.query.{DatasetFactory, QueryExecutionFactory, TxnType}
import org.apache.jena.riot.{Lang, RDFParser, RDFWriter}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.jena.sparql.graph.GraphFactory

import java.util.UUID
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
  * A rooted Graph representation backed up by a Jena DatasetGraph.
  *
  * @param rootNode
  *   the root node of the graph
  * @param value
  *   the Jena dataset graph
  */
final case class Graph private (rootNode: IriOrBNode, value: DatasetGraph) { self =>

  def rootResource: Node = subject(rootNode)

  /**
    * Returns all the triples of the current graph
    */
  def triples: Set[Triple] =
    value.find().asScala.map(Triple(_)).toSet

  /**
    * Returns all the quads of the current graph
    */
  def quads: Set[Quad] =
    value.find().asScala.map(q => (q.getGraph, q.getSubject, q.getPredicate, q.getObject)).toSet

  /**
    * Test if the graph is empty
    * @return
    */
  def isEmpty: Boolean = value.isEmpty

  def getDefaultGraphSize: Int = value.getDefaultGraph.size()

  /**
    * Returns a subgraph retaining all the triples that satisfy the provided predicate.
    */
  def filter(triples: Iterable[Triple]): Graph = {
    val iterator = triples.iterator.flatMap { triple =>
      value.find(Node.ANY, triple._1, triple._2, triple._3).asScala
    }
    val newGraph = DatasetFactory.create().asDatasetGraph()
    iterator.foreach(newGraph.add)
    copy(value = newGraph)
  }

  /**
    * A pair of, first, all triples that satisfy predicate `p` and, second, all triples that do not.
    */
  def partition(evalTriple: Triple => Boolean): (Graph, Graph) = {
    val left     = DatasetFactory.create().asDatasetGraph()
    val right    = DatasetFactory.create().asDatasetGraph()
    val iterator = value.find()
    while iterator.hasNext do {
      val quad = iterator.next()
      if evalTriple(Triple(quad)) then left.add(quad)
      else right.add(quad)
    }
    (Graph(rootNode, left), Graph(rootNode, right))
  }

  /**
    * Returns a triple matching the predicate if found.
    */
  def find(evalTriple: Triple => Boolean): Option[Triple] = {
    val iter = value.find()

    @tailrec
    def inner(result: Option[Triple] = None): Option[Triple] =
      if result.isEmpty && iter.hasNext then {
        val triple = Triple(iter.next())
        inner(Option.when(evalTriple(triple))(triple))
      } else result

    inner()
  }

  /**
    * Returns an object of a triple with a given subject and predicate.
    */
  def find(subject: IriOrBNode, predicate: Iri): Option[Node] = {
    val sNode    = Triple.subject(subject)
    val pNode    = Triple.predicate(predicate)
    val iterator = value.find(Node.ANY, sNode, pNode, Node.ANY)
    Option.when(iterator.hasNext)(iterator.next().getObject)
  }

  /**
    * Replace the rootNode with the passed ''newRootNode''.
    */
  def replaceRootNode(newRootNode: IriOrBNode): Graph =
    replace(rootNode, newRootNode).copy(rootNode = newRootNode)

  /**
    * Replace an [[IriOrBNode]] to another [[IriOrBNode]] on the subject or object positions of the graph.
    *
    * @param current
    *   the current [[IriOrBNode]]
    * @param replace
    *   the replacement when the ''current'' [[IriOrBNode]] is found
    */

  def replace(current: IriOrBNode, replace: IriOrBNode): Graph =
    self.replace(subject(current), subject(replace))

  private def replace(current: Node, replace: Node): Graph = {
    val iter     = value.find()
    val newGraph = DatasetFactory.create().asDatasetGraph()
    while iter.hasNext do {
      val quad      = iter.next
      val (s, p, o) = Triple(quad)
      val ss        = if s == current then replace else s
      val oo        = if o == current then replace else o
      newGraph.add(quad.getGraph, ss, p, oo)
    }
    copy(value = newGraph)
  }

  /**
    * Returns the objects with the predicate ''rdf:type'' and subject ''root''.
    */
  def rootTypes: Set[Iri] = rootTypesNodes.map { t => iri"${t._3.getURI}" }

  /**
    * Returns the triples with the predicate ''rdf:type'' and subject ''root''.
    */
  def rootTypesNodes: Set[Triple] = {
    val iterator           = value.find(Node.ANY, rootResource, predicate(rdf.tpe), Node.ANY)
    def validType(r: Node) = r.isURI && r.getURI != null && r.getURI.nonEmpty
    iterator.asScala
      .filter(q => validType(q.getObject))
      .map(Triple(_))
      .toSet
  }

  /**
    * Adds the passed ''triple'' to the existing graph.
    */
  def add(triple: Triple): Graph = writeTx {
    _.getDefaultGraph.add(triple._1, triple._2, triple._3)
  }

  /**
    * Adds a triple using the current ''root'' subject and the passed predicate ''p'' and object ''o''.
    */
  def add(p: Node, o: Node): Graph =
    add((rootResource, p, o))

  /**
    * Adds a set of triples to the existing graph.
    */
  def add(triple: Set[Triple]): Graph = writeTx { ds =>
    triple.foreach { case (s, p, o) => ds.getDefaultGraph.add(s, p, o) }
  }

  def deleteAny(subject: IriOrBNode, predicate: Iri): Graph = writeTx { ds =>
    ds.deleteAny(Node.ANY, Triple.subject(subject), Triple.predicate(predicate), Node.ANY)
  }

  /**
    * Deletes a set of triples from the existing graph.
    */
  def delete(triples: Set[Triple]): Graph = writeTx { ds =>
    triples.foreach { triple =>
      ds.getDefaultGraph.delete(triple._1, triple._2, triple._3)
    }
  }

  /**
    * Attempts to convert the current Graph to the N-Triples format: https://www.w3.org/TR/n-triples/
    */
  def toNTriples: IO[NTriples] =
    tryExpensiveIO(
      RDFWriter.create().lang(Lang.NTRIPLES).source(collapseGraphs).asString(),
      Lang.NTRIPLES.getName
    ).map(NTriples(_, rootNode))

  /**
    * Attempts to convert the current Graph to the N-Quads format: https://www.w3.org/TR/n-quads/
    */
  def toNQuads: IO[NQuads] =
    tryExpensiveIO(
      RDFWriter.create().lang(Lang.NQUADS).source(value).asString(),
      Lang.NQUADS.getName
    ).map(NQuads(_, rootNode))

  /**
    * Transform the current graph to a new one via the provided construct query
    * @param query
    *   the construct query
    */
  def transform(query: SparqlConstructQuery): Either[SparqlConstructQueryError, Graph] =
    Try {
      val qe = QueryExecutionFactory.create(query.jenaQuery, value)
      Graph(rootNode, qe.execConstructDataset().asDatasetGraph())
    }.toEither.leftMap { e =>
      SparqlConstructQueryError(query, rootNode, e.getMessage)
    }

  /**
    * Attempts to convert the current Graph with the passed ''context'' value to the DOT format:
    * https://graphviz.org/doc/info/lang.html
    *
    * The context will be inspected to populate its fields and then the conversion will be performed.
    */
  def toDot(
      contextValue: ContextValue = ContextValue.empty
  )(using RemoteContextResolution): IO[Dot] =
    for {
      resolvedCtx <- JsonLdContext(contextValue)
      ctx          = dotContext(rootResource, resolvedCtx)
      string      <-
        tryExpensiveIO(RDFWriter.create().lang(DOT).source(collapseGraphs).context(ctx).asString(), DOT.getName)
    } yield Dot(string, rootNode)

  /**
    * Attempts to convert the current Graph with the passed ''context'' value to the JSON-LD compacted format:
    * https://www.w3.org/TR/json-ld11-api/#compaction-algorithms
    *
    * Note: This is done in two steps, first transforming the graph to JSON-LD expanded format and then compacting it.
    */
  def toCompactedJsonLd(
      contextValue: ContextValue
  )(using api: JsonLdApi, rcr: RemoteContextResolution): IO[CompactedJsonLd] = {
    def computeCompacted(id: IriOrBNode, input: Json): IO[CompactedJsonLd] = {
      if isEmpty then IO.delay(CompactedJsonLd.unsafe(id, contextValue, JsonObject.empty))
      else if value.listGraphNodes().hasNext then {
        CompactedJsonLd(id, contextValue, input)
      } else {
        CompactedJsonLd.frame(id, contextValue, input)
      }
    }

    if rootNode.isBNode then
      for {
        expanded <- api.fromRdf(replace(rootNode, fakeId).value)
        framed   <- computeCompacted(fakeId, expanded.asJson)
      } yield framed.replaceId(self.rootNode)
    else api.fromRdf(value).flatMap(expanded => computeCompacted(rootNode, expanded.asJson))
  }

  /**
    * Merges the current graph with the passed ''that'' while keeping the current ''rootNode''
    */
  def ++(that: Graph): Graph =
    writeTx { _.addAll(that.value) }

  /**
    * Removes the passed graph triples from the current [[Graph]]
    */
  def --(graph: Graph): Graph =
    writeTx { ds =>
      val iterator = graph.value.find().asScala
      iterator.foreach(ds.delete)
    }

  override def hashCode(): Int = (rootNode, quads).##

  override def equals(obj: Any): Boolean =
    obj.asInstanceOf[Matchable] match {
      case that: Graph if rootNode.isBNode && that.rootNode.isBNode => quads == that.quads
      case that: Graph                                              => rootNode == that.rootNode && quads == that.quads
      case _                                                        => false
    }

  override def toString: String =
    s"rootNode = '$rootNode', triples = '$triples'"

  private def collapseGraphs =
    if value.listGraphNodes().hasNext then {
      val newGraph = GraphFactory.createDefaultGraph()
      value.find().asScala.foreach(quad => newGraph.add(quad.asTriple()))
      newGraph
    } else value.getDefaultGraph

  private def writeTx(f: DatasetGraph => Unit): Graph = {
    value.begin(TxnType.WRITE)
    try {
      f(value)
      value.commit()
      this
    } finally {
      value.end()
    }
  }

}

object Graph {

  // This fake id is used in cases where the ''rootNode'' is a Blank Node.
  // Since a Blank Node is ephemeral, its value can change on any conversion Json-LD <-> Graph.
  // We replace the Blank Node for the fakeId, do the conversion and then replace the fakeId with the Blank Node again.
  private[graph] val fakeId = iri"http://localhost/${UUID.randomUUID()}"

  val rdfType: Node = predicate(rdf.tpe)

  /**
    * An empty graph with a auto generated [[BNode]] as a root node
    */
  final def empty: Graph = empty(BNode.random)

  /**
    * An empty graph with the passed ''rootNode''.
    */
  final def empty(rootNode: IriOrBNode): Graph = Graph(rootNode, DatasetFactory.create().asDatasetGraph())

  /**
    * Creates a [[Graph]] from an expanded JSON-LD.
    *
    * @param expanded
    *   the expanded JSON-LD input to transform into a Graph
    */
  final def apply(
      expanded: ExpandedJsonLd
  )(using api: JsonLdApi): IO[Graph] =
    (expanded.obj(keywords.graph), expanded.rootId) match {
      case (Some(_), _: BNode) =>
        IO.raiseError(UnexpectedJsonLd("Expected named graph, but root @id not found"))
      case (Some(_), iri: Iri) =>
        api.toRdf(expanded.json).map(g => Graph(iri, g))
      case (None, _: BNode)    =>
        val json = expanded.replaceId(fakeId).json
        api.toRdf(json).map(g => Graph(expanded.rootId, g).replace(fakeId, expanded.rootId))
      case (None, iri: Iri)    =>
        api.toRdf(expanded.json).map(g => Graph(iri, g))
    }

  def apply(graph: Graph): Graph = {
    val newGraph = DatasetFactory.create().asDatasetGraph()
    newGraph.addAll(graph.value)
    graph.value.find().asScala.foreach(newGraph.add)
    Graph(graph.rootNode, newGraph)
  }

  /**
    * Generates a [[Graph]] from the passed ''nTriples'' representation
    */
  final def apply(nTriples: NTriples): Either[ParsingError, Graph] = {
    val g = DatasetFactory.create().asDatasetGraph()
    Try(RDFParser.create().fromString(nTriples.value).lang(Lang.NTRIPLES).parse(g)).toEither
      .leftMap(err => ParsingError(err.getMessage, nTriples))
      .as(Graph(nTriples.rootNode, g))
  }

  /**
    * Generates a [[Graph]] from the passed ''nQuads'' representation
    */
  final def apply(nQuads: NQuads): Either[ParsingError, Graph] = {
    val g = DatasetFactory.create().asDatasetGraph()
    Try(RDFParser.create().fromString(nQuads.value).lang(Lang.NQUADS).parse(g)).toEither
      .leftMap(err => ParsingError(err.getMessage, nQuads))
      .as(Graph(nQuads.rootNode, g))
  }

  /**
    * Unsafely builds a graph from an already passed [[DatasetGraph]] and an auto generated [[BNode]] as a root node
    */
  final def unsafe(graph: DatasetGraph): Graph =
    unsafe(BNode.random, graph)

  /**
    * Unsafely builds a graph from an already passed [[DatasetGraph]] and the passed [[IriOrBNode]]
    */
  final def unsafe(rootNode: IriOrBNode, graph: DatasetGraph): Graph =
    Graph(rootNode, graph)

}
