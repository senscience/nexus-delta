package ai.senscience.nexus.delta.sdk.jsonld

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.{ExplainResult, RdfError}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import org.typelevel.otel4s.trace.Tracer

/**
  * Allows to define different JsonLd processors
  */
sealed abstract class JsonLdSourceProcessor {

  given JsonLdApi = TitaniumJsonLdApi.strict

  def uuidF: UUIDF

  protected def getOrGenerateId(iri: Option[Iri], context: ProjectContext): IO[Iri] =
    iri.fold(uuidF().map(uuid => context.base.iri / uuid.toString))(IO.pure)

  /**
    * Expand the source document using the provided project context and remote context resolution.
    *
    * If the source does not provide a context, one will be injected from the project base and vocab.
    */
  protected def expandSource(projectContext: ProjectContext, source: Json)(using
      RemoteContextResolution
  ): IO[(ContextValue, ExplainResult[ExpandedJsonLd])] = {
    given JsonLdOptions = JsonLdOptions(base = Some(projectContext.base.iri))
    val sourceContext   = source.topContextValueOrEmpty
    if sourceContext.isEmpty then {
      val defaultContext = defaultCtx(projectContext)
      ExpandedJsonLd.explain(source.addContext(defaultContext.contextObj)).map(defaultContext -> _)
    } else ExpandedJsonLd.explain(source).map(sourceContext -> _)
  }.adaptError { case err: RdfError => InvalidJsonLdFormat(None, err) }

  protected def checkAndSetSameId(iri: Iri, expanded: ExpandedJsonLd): IO[ExpandedJsonLd] =
    expanded.rootId match {
      case _: BNode        => IO.pure(expanded.replaceId(iri))
      case `iri`           => IO.pure(expanded)
      case payloadIri: Iri => IO.raiseError(UnexpectedId(iri, payloadIri))
    }

  private def defaultCtx(context: ProjectContext): ContextValue =
    ContextObject(JsonObject(keywords.vocab -> context.vocab.asJson, keywords.base -> context.base.asJson))

}

object JsonLdSourceProcessor {

  private def checkIdNotBlank(source: Json): IO[Unit] =
    IO.raiseWhen(source.hcursor.downField("@id").as[String].exists(_.isBlank))(BlankId)

  /**
    * Allows to parse the given json source to JsonLD compacted and expanded using static contexts
    */
  final private class JsonLdSourceParser(contextIri: Seq[Iri], override val uuidF: UUIDF)(using Tracer[IO])
      extends JsonLdSourceProcessor {

    /**
      * Converts the passed ''source'' to JsonLD compacted and expanded. The @id value is extracted from the payload.
      * When no @id is present, one is generated using the base on the project suffixed with a randomly generated UUID.
      *
      * @param context
      *   the project context with the base used to generate @id when needed and the @context when not provided on the
      *   source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the resulting @id iri, the compacted Json-LD and the expanded Json-LD
      */
    def apply(
        context: ProjectContext,
        source: Json
    )(using RemoteContextResolution): IO[JsonLdAssembly] = {
      for {
        _               <- checkIdNotBlank(source)
        (ctx, result)   <- expandSource(context, source.addContext(contextIri*))
        originalExpanded = result.value
        iri             <- getOrGenerateId(originalExpanded.rootId.asIri, context)
        expanded         = originalExpanded.replaceId(iri)
        assembly        <- JsonLdAssembly(iri, source, expanded, ctx, result.remoteContexts)
      } yield assembly
    }.surround("convertToJsonLd")

    /**
      * Converts the passed ''source'' to JsonLD compacted and expanded. The @id value is extracted from the payload if
      * exists and compared to the passed ''iri''. If they aren't equal an [[UnexpectedId]] rejection is issued.
      *
      * @param context
      *   the project context to generate the @context when no @context is provided on the source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the compacted Json-LD and the expanded Json-LD
      */
    def apply(
        context: ProjectContext,
        iri: Iri,
        source: Json
    )(using RemoteContextResolution): IO[JsonLdAssembly] = {
      for {
        _               <- checkIdNotBlank(source)
        (ctx, result)   <- expandSource(context, source.addContext(contextIri*))
        originalExpanded = result.value
        expanded        <- checkAndSetSameId(iri, originalExpanded)
        assembly        <- JsonLdAssembly(iri, source, expanded, ctx, result.remoteContexts)
      } yield assembly
    }.surround("convertToJsonLd")

  }

  /**
    * Allows to parse the given json source to JsonLD compacted and expanded using static and resolver-based contexts
    */
  final class JsonLdSourceResolvingParser(
      contextIri: Seq[Iri],
      contextResolution: ResolverContextResolution,
      override val uuidF: UUIDF
  )(using Tracer[IO])
      extends JsonLdSourceProcessor {

    private val underlying = new JsonLdSourceParser(contextIri, uuidF)

    /**
      * Converts the passed ''source'' to JsonLD compacted and expanded. The @id value is extracted from the payload.
      * When no @id is present, one is generated using the base on the project suffixed with a randomly generated UUID.
      *
      * @param ref
      *   the project reference
      * @param context
      *   the project context with the base used to generate @id when needed and the @context when not provided on the
      *   source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the resulting @id iri, the compacted Json-LD and the expanded Json-LD
      */
    def apply(ref: ProjectRef, context: ProjectContext, source: Json)(using Caller): IO[JsonLdAssembly] = {
      given RemoteContextResolution = contextResolution(ref)
      underlying(context, source)
    }

    /**
      * Converts the passed ''source'' to JsonLD compacted and expanded. The @id value is extracted from the payload if
      * exists and compared to the passed ''iri''. If they aren't equal an [[UnexpectedId]] rejection is issued.
      *
      * @param ref
      *   the project reference
      * @param context
      *   the project context to generate the @context when no @context is provided on the source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the compacted Json-LD and the expanded Json-LD
      */
    def apply(
        ref: ProjectRef,
        context: ProjectContext,
        iri: Iri,
        source: Json
    )(using Caller): IO[JsonLdAssembly] = {
      given RemoteContextResolution = contextResolution(ref)
      underlying(context, iri, source)
    }

    /**
      * Converts the passed ''source'' to JsonLD compacted and expanded. The @id value is extracted from the payload if
      * exists and compared to the passed ''iri'' if defined. If they aren't equal an [[UnexpectedId]] rejection is
      * issued.
      *
      * When no @id is present, one is generated using the base on the project suffixed with a randomly generated UUID.
      *
      * @param ref
      *   the project reference
      * @param context
      *   the project context to generate the @context when no @context is provided on the source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the compacted Json-LD and the expanded Json-LD
      */
    def apply(ref: ProjectRef, context: ProjectContext, iriOpt: Option[Iri], source: Json)(using
        Caller
    ): IO[JsonLdAssembly] =
      iriOpt
        .map { iri =>
          apply(ref, context, iri, source)
        }
        .getOrElse {
          apply(ref, context, source)
        }
  }

  object JsonLdSourceResolvingParser {
    def apply(contextResolution: ResolverContextResolution, uuidF: UUIDF)(using
        Tracer[IO]
    ): JsonLdSourceResolvingParser =
      new JsonLdSourceResolvingParser(Seq.empty, contextResolution, uuidF)
  }

  /**
    * Allows to parse the given json source and decode it into an ''A'' using static contexts
    */
  final class JsonLdSourceDecoder[A: JsonLdDecoder](contextIri: Iri, override val uuidF: UUIDF)
      extends JsonLdSourceProcessor {

    /**
      * Expands the passed ''source'' and attempt to decode it into an ''A'' The @id value is extracted from the
      * payload. When no @id is present, one is generated using the base on the project suffixed with a randomly
      * generated UUID.
      *
      * @param context
      *   the project context with the base used to generate @id when needed and the @context when not provided on the
      *   source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the resulting @id iri and the decoded value
      */
    def apply(context: ProjectContext, source: Json)(using RemoteContextResolution): IO[(Iri, A)] = {
      for {
        (_, result)  <- expandSource(context, source.addContext(contextIri))
        expanded      = result.value
        iri          <- getOrGenerateId(expanded.rootId.asIri, context)
        decodedValue <- IO.fromEither(expanded.to[A].leftMap(DecodingFailed(_)))
      } yield (iri, decodedValue)
    }

    /**
      * Expands the passed ''source'' and attempt to decode it into an ''A'' The @id value is extracted from the payload
      * if exists and compared to the passed ''iri''. If they aren't equal an [[UnexpectedId]] rejection is issued.
      *
      * @param context
      *   the project context with the base used to generate @id when needed and the @context when not provided on the
      *   source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the resulting @id iri and the decoded value
      */
    def apply(context: ProjectContext, iri: Iri, source: Json)(using RemoteContextResolution): IO[A] =
      for {
        (_, result)     <- expandSource(context, source.addContext(contextIri))
        originalExpanded = result.value
        expanded        <- checkAndSetSameId(iri, originalExpanded)
        decodedValue    <- IO.fromEither(expanded.to[A].leftMap(DecodingFailed(_)))
      } yield decodedValue
  }

  /**
    * Allows to parse the given json source and decode it into an ''A'' using static and resolver-based contexts
    */
  final class JsonLdSourceResolvingDecoder[A: JsonLdDecoder](
      contextIri: Iri,
      contextResolution: ResolverContextResolution,
      override val uuidF: UUIDF
  ) extends JsonLdSourceProcessor {

    private val underlying = new JsonLdSourceDecoder[A](contextIri, uuidF)

    /**
      * Expands the passed ''source'' and attempt to decode it into an ''A'' The @id value is extracted from the
      * payload. When no @id is present, one is generated using the base on the project suffixed with a randomly
      * generated UUID.
      *
      * @param ref
      *   the project reference
      * @param context
      *   the project context with the base used to generate @id when needed and the @context when not provided on the
      *   source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the resulting @id iri and the decoded value
      */
    def apply(ref: ProjectRef, context: ProjectContext, source: Json)(using Caller): IO[(Iri, A)] = {
      given RemoteContextResolution = contextResolution(ref)
      underlying(context, source)
    }

    /**
      * Expands the passed ''source'' and attempt to decode it into an ''A'' The @id value is extracted from the payload
      * if exists and compared to the passed ''iri''. If they aren't equal an [[UnexpectedId]] rejection is issued.
      *
      * @param ref
      *   the project reference
      * @param context
      *   the project context with with the base used to generate @id when needed and the @context when not provided on
      *   the source
      * @param source
      *   the Json payload
      * @return
      *   a tuple with the resulting @id iri and the decoded value
      */
    def apply(ref: ProjectRef, context: ProjectContext, iri: Iri, source: Json)(using Caller): IO[A] = {
      given RemoteContextResolution = contextResolution(ref)
      underlying(context, iri, source)
    }
  }

}
