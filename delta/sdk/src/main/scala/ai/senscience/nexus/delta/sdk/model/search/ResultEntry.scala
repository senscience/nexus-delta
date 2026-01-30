package ai.senscience.nexus.delta.sdk.model.search

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import cats.Functor
import cats.syntax.functor.*
import io.circe.syntax.*
import io.circe.{Encoder, Json}

/**
  * Defines the signature for a single instance result entry
  *
  * @tparam A
  *   the type of the result
  */
sealed trait ResultEntry[A] extends Product with Serializable {

  /**
    * @return
    *   the query result value
    */
  def source: A
}

object ResultEntry {

  /**
    * A result entry with a score
    *
    * @param score
    *   the resulting score for the entry
    * @param source
    *   the query result value
    */
  final case class ScoredResultEntry[A](score: Float, source: A) extends ResultEntry[A]

  /**
    * A result entry without a score
    *
    * @param source
    *   the query result value
    */
  final case class UnscoredResultEntry[A](source: A) extends ResultEntry[A]

  given Functor[ResultEntry] =
    new Functor[ResultEntry] {
      override def map[A, B](fa: ResultEntry[A])(f: A => B): ResultEntry[B] =
        fa match {
          case sqr: ScoredResultEntry[A]   => sqr.map(f)
          case uqr: UnscoredResultEntry[A] => uqr.map(f)
        }
    }

  given Functor[ScoredResultEntry] =
    new Functor[ScoredResultEntry] {
      override def map[A, B](fa: ScoredResultEntry[A])(f: A => B): ScoredResultEntry[B] =
        fa.copy(source = f(fa.source))
    }

  given Functor[UnscoredResultEntry] =
    new Functor[UnscoredResultEntry] {
      override def map[A, B](fa: UnscoredResultEntry[A])(f: A => B): UnscoredResultEntry[B] =
        fa.copy(source = f(fa.source))
    }

  given [A: Encoder.AsObject] => Encoder.AsObject[ResultEntry[A]] =
    Encoder.AsObject.instance {
      case ScoredResultEntry(score, source) =>
        source.asJsonObject.add(nxv.score.prefix, Json.fromFloatOrNull(score))
      case UnscoredResultEntry(source)      => source.asJsonObject
    }
}
