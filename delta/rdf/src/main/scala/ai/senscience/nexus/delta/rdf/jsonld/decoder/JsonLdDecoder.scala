package ai.senscience.nexus.delta.rdf.jsonld.decoder

import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure.KeyMissingFailure
import ai.senscience.nexus.delta.rdf.jsonld.{ExpandedJsonLd, ExpandedJsonLdCursor}
import cats.Order
import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.all.*
import io.circe.parser.parse
import io.circe.{Json, JsonObject}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.Try

trait JsonLdDecoder[A] { self =>

  /**
    * Converts a [[ExpandedJsonLdCursor]] to ''A''.
    *
    * @param cursor
    *   the cursor from the [[ExpandedJsonLd]] document
    */
  def apply(cursor: ExpandedJsonLdCursor): Either[JsonLdDecoderError, A]

  /**
    * Converts a [[ExpandedJsonLd]] document to ''A''.
    */
  def apply(expanded: ExpandedJsonLd): Either[JsonLdDecoderError, A] =
    apply(ExpandedJsonLdCursor(expanded))

  /**
    * Map a function over this [[JsonLdDecoder]].
    */
  final def map[B](f: A => B): JsonLdDecoder[B] = (cursor: ExpandedJsonLdCursor) => self(cursor).map(f)

  /**
    * FlatMap a function over this [[JsonLdDecoder]].
    */
  final def flatMap[B](f: A => Either[JsonLdDecoderError, B]): JsonLdDecoder[B] = (cursor: ExpandedJsonLdCursor) =>
    self(cursor).flatMap(f)

  /**
    * Choose the first succeeding decoder.
    */
  final def or[AA >: A](other: => JsonLdDecoder[AA]): JsonLdDecoder[AA] = (cursor: ExpandedJsonLdCursor) =>
    self(cursor) orElse other(cursor)

  /**
    * @tparam AA
    *   the value supertype
    * @return
    *   a new decoder for a supertype of A
    */
  final def covary[AA >: A]: JsonLdDecoder[AA] =
    map(identity)

  /**
    * Chains a new decoder that uses this cursor and the decoded value to attempt to produce a new value. The cursor
    * retains its history for providing better error messages.
    *
    * @param f
    *   the function that continues the decoding
    */
  final def andThen[B](f: (ExpandedJsonLdCursor, A) => Either[JsonLdDecoderError, B]): JsonLdDecoder[B] =
    (cursor: ExpandedJsonLdCursor) => {
      self(cursor) match {
        case Left(err)    => Left(err)
        case Right(value) => f(cursor, value)
      }
    }

}

object JsonLdDecoder {

  def apply[A](using A: JsonLdDecoder[A]): JsonLdDecoder[A] = A

  private val relativeOrAbsoluteIriDecoder: JsonLdDecoder[Iri] = _.get[Iri](keywords.id)

  given iriJsonLdDecoder: JsonLdDecoder[Iri] =
    relativeOrAbsoluteIriDecoder.andThen { (cursor, iri) =>
      Either.cond(iri.isReference, iri, ParsingFailure("AbsoluteIri", iri.toString, cursor.history))
    }

  given bNodeJsonLdDecoder: JsonLdDecoder[BNode]     = _ => Right(BNode.random)
  given iOrBJsonLdDecoder: JsonLdDecoder[IriOrBNode] =
    iriJsonLdDecoder.or(bNodeJsonLdDecoder.map[IriOrBNode](identity))

  given stringJsonLdDecoder: JsonLdDecoder[String]   = _.get[String](keywords.value)
  given intJsonLdDecoder: JsonLdDecoder[Int]         = _.getOr(keywords.value, _.toIntOption)
  given longJsonLdDecoder: JsonLdDecoder[Long]       = _.getOr(keywords.value, _.toLongOption)
  given doubleJsonLdDecoder: JsonLdDecoder[Double]   = _.getOr(keywords.value, _.toDoubleOption)
  given floatJsonLdDecoder: JsonLdDecoder[Float]     = _.getOr(keywords.value, _.toFloatOption)
  given booleanJsonLdDecoder: JsonLdDecoder[Boolean] = _.getOr(keywords.value, _.toBooleanOption)

  given instantJsonLdDecoder: JsonLdDecoder[Instant]               = _.getValueTry(Instant.parse)
  given uuidJsonLdDecoder: JsonLdDecoder[UUID]                     = _.getValueTry(UUID.fromString)
  given durationJsonLdDecoder: JsonLdDecoder[Duration]             = _.getValueTry(Duration.apply)
  given finiteDurationJsonLdDecoder: JsonLdDecoder[FiniteDuration] =
    _.getValue(str => Try(Duration(str)).toOption.collectFirst { case f: FiniteDuration => f })

  given vectorJsonLdDecoder: [A: JsonLdDecoder] => JsonLdDecoder[Vector[A]] = listJsonLdDecoder[A].map(_.toVector)

  given setJsonLdDecoder: [A] => (dec: JsonLdDecoder[A]) => JsonLdDecoder[Set[A]] =
    cursor => cursor.values.flatMap(innerCursors => innerCursors.traverse(dec(_))).map(_.toSet)

  given nonEmptySetJsonLdDecoder: [A: {JsonLdDecoder, Order}] => (A: ClassTag[A]) => JsonLdDecoder[NonEmptySet[A]] =
    setJsonLdDecoder[A].flatMap { s =>
      s.toList match {
        case ::(head, tail) => Right(NonEmptySet.of(head, tail*))
        case Nil            => Left(ParsingFailure(s"Expected a NonEmptySet[${A.simpleName}], but the current set is empty"))
      }
    }

  given listJsonLdDecoder: [A] => (dec: JsonLdDecoder[A]) => JsonLdDecoder[List[A]] =
    cursor => cursor.downList.values.flatMap(innerCursors => innerCursors.traverse(dec(_)))

  given nonEmptyListsonLdDecoder: [A: JsonLdDecoder] => (A: ClassTag[A]) => JsonLdDecoder[NonEmptyList[A]] =
    listJsonLdDecoder[A].flatMap {
      case Nil          => Left(ParsingFailure(s"Expected a NonEmptyList[${A.simpleName}], but the current list is empty"))
      case head :: tail => Right(NonEmptyList(head, tail))
    }

  given optionJsonLdDecoder: [A] => (dec: JsonLdDecoder[A]) => JsonLdDecoder[Option[A]] =
    cursor =>
      if cursor.succeeded then
        dec(cursor).map(Some.apply).recover {
          case k: KeyMissingFailure if k.path.isEmpty =>
            None
        }
      else Right(None)

  // assumes the field is encoded as a string
  // TODO: remove when `@type: json` is supported by the json-ld lib
  given jsonObjectJsonLdDecoder: JsonLdDecoder[JsonObject] = _.getValue(parse(_).toOption.flatMap(_.asObject))

  // assumes the field is encoded as a string
  // TODO: remove when `@type: json` is supported by the json-ld lib
  given jsonJsonLdDecoder: JsonLdDecoder[Json] = _.getValue(parse(_).toOption)

  given unitJsonLdDecoder: JsonLdDecoder[Unit] = _ => Right(())
}
