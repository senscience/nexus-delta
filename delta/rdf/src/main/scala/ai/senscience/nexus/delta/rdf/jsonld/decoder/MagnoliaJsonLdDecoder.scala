package ai.senscience.nexus.delta.rdf.jsonld.decoder

import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLdCursor
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.DecodingDerivationFailure
import magnolia1.*

import scala.annotation.nowarn
import scala.deriving.*

private[decoder] object MagnoliaJsonLdDecoder { self =>

  @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
  inline def derived[A](using config: Configuration, inline m: Mirror.Of[A]): JsonLdDecoder[A] = {
    val derivation: Derivation[JsonLdDecoder] = new Derivation[JsonLdDecoder] {

      override def split[T](ctx: SealedTrait[Typeclass, T]): Typeclass[T] =
        self.split[T](ctx)

      override def join[T](ctx: CaseClass[Typeclass, T]): Typeclass[T] =
        self.join[T](ctx)
    }
    derivation.derived[A]
  }

  def join[A](caseClass: CaseClass[JsonLdDecoder, A])(using config: Configuration): JsonLdDecoder[A] = {
    val keysMap = caseClass.parameters
      .filter { p => p.label != config.idPredicateName }
      .map { p =>
        p.label -> config.context
          .expand(p.label, useVocab = true)
          .toRight {
            val err = s"Label '${p.label}' could not be converted to Iri. Please provide a correct context"
            DecodingDerivationFailure(err)
          }
      }
      .toMap

    new JsonLdDecoder[A] {

      override def apply(cursor: ExpandedJsonLdCursor): Either[JsonLdDecoderError, A] =
        caseClass.constructMonadic {
          case p if p.label == config.idPredicateName => decodeOrDefault(p.typeclass, cursor, p.default)
          case p                                      =>
            keysMap(p.label).flatMap { predicate =>
              val nextCursor = cursor.downField(predicate)
              decodeOrDefault(p.typeclass, nextCursor, p.default)
            }
        }

      private def decodeOrDefault[B](dec: JsonLdDecoder[B], cursor: ExpandedJsonLdCursor, default: Option[B]) =
        default match {
          case Some(value) if !cursor.succeeded => Right(value)
          case _                                => dec(cursor)
        }
    }
  }

  def split[A](sealedTrait: SealedTrait[JsonLdDecoder, A])(using config: Configuration): JsonLdDecoder[A] =
    (c: ExpandedJsonLdCursor) =>
      c.getTypes match {
        case Right(types) =>
          sealedTrait.subtypes
            .find(st => types.exists(config.context.expand(st.typeInfo.short, useVocab = true).contains)) match {
            case Some(st) => st.typeclass.apply(c)
            case None     =>
              val err = s"Unable to find type discriminator for '${sealedTrait.typeInfo.short}'"
              Left(DecodingDerivationFailure(err))
          }
        case Left(err)    => Left(err)
      }
}
