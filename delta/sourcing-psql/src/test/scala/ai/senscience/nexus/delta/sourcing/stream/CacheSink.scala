package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import shapeless.Typeable

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.Set as MutableSet
import scala.concurrent.duration.*

final class CacheSink[A: Typeable] private (documentId: Elem[A] => Iri) extends Sink {

  val successes: mutable.Map[Iri, A] = TrieMap.empty[Iri, A]
  val dropped: MutableSet[Iri]       = MutableSet.empty[Iri]
  val failed: MutableSet[Iri]        = MutableSet.empty[Iri]

  override type In = A

  override def inType: Typeable[A] = Typeable[A]

  override def apply(elements: ElemChunk[A]): IO[ElemChunk[Unit]] = IO.delay {
    elements.map {
      case s: SuccessElem[A] =>
        successes.put(documentId(s), s.value)
        s.void
      case d: DroppedElem    =>
        dropped.add(documentId(d))
        d
      case f: FailedElem     =>
        failed.add(documentId(f))
        f
    }
  }

  override val batchConfig: BatchConfig = BatchConfig(1, 10.millis)

}

object CacheSink {
  private val eventDocumentId: Elem[?] => Iri =
    elem => iri"${elem.project}/${elem.id}:${elem.rev}"

  /** CacheSink for events */
  def events[A: Typeable]: CacheSink[A] = new CacheSink(eventDocumentId)

  /** CacheSink for states */
  def states[A: Typeable]: CacheSink[A] = new CacheSink(_.id)
}
