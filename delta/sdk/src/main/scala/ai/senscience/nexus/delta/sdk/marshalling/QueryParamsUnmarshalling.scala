package ai.senscience.nexus.delta.sdk.marshalling

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, JsonLdContext}
import ai.senscience.nexus.delta.sdk.implicits.{given, *}
import ai.senscience.nexus.delta.sdk.marshalling.QueryParamsUnmarshalling.{IriBase, IriVocab}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import io.circe.Json
import io.circe.parser.parse
import org.apache.pekko.http.scaladsl.unmarshalling.{FromStringUnmarshaller, Unmarshaller}

/**
  * Unmarshallers from String to ''A''
  */
trait QueryParamsUnmarshalling {

  /**
    * Unmarshaller to transform a String to Iri
    */
  given iriFromStringUnmarshaller: FromStringUnmarshaller[Iri] =
    Unmarshaller.strict[String, Iri] { string =>
      Iri(string) match {
        case Right(iri) => iri
        case Left(err)  => throw new IllegalArgumentException(err)
      }
    }

  /**
    * Unmarshaller to transform a String to an IriBase
    */
  val iriBaseFromStringUnmarshallerNoExpansion: FromStringUnmarshaller[IriBase] =
    iriFromStringUnmarshaller.map(IriBase(_))

  /**
    * Unmarsaller to transform a String to an IriVocab
    */
  given iriVocabFromStringUnmarshaller: ProjectContext => FromStringUnmarshaller[IriVocab] =
    expandIriFromStringUnmarshaller(useVocab = true).map(IriVocab.apply)

  /**
    * Unmarshaller to transform a String to an IriBase
    */
  given iriBaseFromStringUnmarshaller: ProjectContext => FromStringUnmarshaller[IriBase] =
    expandIriFromStringUnmarshaller(useVocab = false).map(IriBase(_))

  private def expandIriFromStringUnmarshaller(
      useVocab: Boolean
  )(using pc: ProjectContext): FromStringUnmarshaller[Iri] =
    Unmarshaller.strict[String, Iri] { str =>
      val ctx = context(pc.vocab, pc.base.iri, pc.apiMappings)
      ctx.expand(str, useVocab = useVocab) match {
        case Some(iri) => iri
        case None      => throw new IllegalArgumentException(s"'$str' cannot be expanded to an Iri")

      }
    }

  private def context(vocab: Iri, base: Iri, mappings: ApiMappings): JsonLdContext =
    JsonLdContext(
      ContextValue.empty,
      base = Some(base),
      vocab = Some(vocab),
      prefixMappings = mappings.prefixMappings,
      aliases = mappings.aliases
    )

  /**
    * Unmarshaller to transform a String to Label
    */
  given labelFromStringUnmarshaller: FromStringUnmarshaller[Label] =
    Unmarshaller.strict[String, Label] { string =>
      Label(string) match {
        case Right(iri) => iri
        case Left(err)  => throw new IllegalArgumentException(err.getMessage)
      }
    }

  given projectRefFromStringUnmarshaller: FromStringUnmarshaller[ProjectRef] =
    Unmarshaller.strict[String, ProjectRef] { string =>
      ProjectRef.parse(string) match {
        case Right(iri) => iri
        case Left(err)  => throw new IllegalArgumentException(err)
      }
    }

  /**
    * Unmarshaller to transform a String to TagLabel
    */
  given tagLabelFromStringUnmarshaller: FromStringUnmarshaller[UserTag] =
    Unmarshaller.strict[String, UserTag] { string =>
      UserTag(string) match {
        case Right(tagLabel) => tagLabel
        case Left(err)       => throw new IllegalArgumentException(err.message)
      }
    }

  given permissionFromStringUnmarshaller: FromStringUnmarshaller[Permission] =
    Unmarshaller.strict[String, Permission] { string =>
      Permission(string) match {
        case Right(value) => value
        case Left(err)    => throw new IllegalArgumentException(err.getMessage)
      }
    }

  /**
    * Unmarshaller to transform an Iri to a Subject
    */
  given subjectFromIriUnmarshaller: BaseUri => Unmarshaller[Iri, Subject] =
    Unmarshaller.strict[Iri, Subject] { iri =>
      iri.as[Subject] match {
        case Right(subject) => subject
        case Left(err)      => throw new IllegalArgumentException(err.getMessage)
      }
    }

  /**
    * Unmarshaller to transform a String to a Subject
    */
  given subjectFromStringUnmarshaller: BaseUri => FromStringUnmarshaller[Subject] =
    iriFromStringUnmarshaller.andThen(subjectFromIriUnmarshaller)

  /**
    * Unmarshaller to transform a String to an IdSegment
    */
  given idSegmentFromStringUnmarshaller: FromStringUnmarshaller[IdSegment] =
    Unmarshaller.strict[String, IdSegment](IdSegment.apply)

  given jsonFromStringUnmarshaller: FromStringUnmarshaller[Json] =
    Unmarshaller.strict[String, Json](parse(_).fold(throw _, identity))

}

object QueryParamsUnmarshalling extends QueryParamsUnmarshalling {

  /**
    * An Iri generated using the vocab when there is no alias or curie suited for it
    */
  final case class IriVocab private[sdk] (value: Iri) extends AnyVal

  /**
    * An Iri generated using the base when there is no alias or curie suited for it
    */
  final case class IriBase(value: Iri) extends AnyVal
}
