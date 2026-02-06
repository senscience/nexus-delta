package ai.senscience.nexus.delta.rdf.jsonld.api

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.RdfError.{ConversionError, RemoteContextError, UnexpectedJsonLd, UnexpectedJsonLdContext}
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdApiConfig.ErrorHandling
import ai.senscience.nexus.delta.rdf.jsonld.api.TitaniumJsonLdApi.tryExpensiveIO
import ai.senscience.nexus.delta.rdf.jsonld.context.*
import ai.senscience.nexus.delta.rdf.{ExplainResult, RdfError}
import cats.effect.IO
import cats.syntax.all.*
import com.apicatalog.jsonld.JsonLdOptions.RdfDirection
import com.apicatalog.jsonld.context.ActiveContext
import com.apicatalog.jsonld.document.JsonDocument
import com.apicatalog.jsonld.loader.DocumentLoader
import com.apicatalog.jsonld.processor.ProcessingRuntime
import com.apicatalog.jsonld.uri.UriValidationPolicy
import com.apicatalog.jsonld.{JsonLd, JsonLdError, JsonLdErrorCode, JsonLdOptions as TitaniumJsonLdOptions}
import io.circe.jakartajson.*
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import jakarta.json.JsonStructure
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.jena.irix.IRIxResolver
import org.apache.jena.query.DatasetFactory
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.system.*
import org.apache.jena.riot.system.jsonld.{JenaToTitanium, TitaniumToJena}
import org.apache.jena.sparql.core.DatasetGraph

import java.net.URI
import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
  * Json-LD high level API implementation by Json-LD Java library
  */
final class TitaniumJsonLdApi(config: JsonLdApiConfig, opts: JsonLdOptions) extends JsonLdApi {

  private def circeToDocument(json: Json) =
    circeToJakarta(json) match {
      case structure: JsonStructure => JsonDocument.of(structure)
      case _                        =>
        throw new JsonLdError(
          JsonLdErrorCode.LOADING_DOCUMENT_FAILED,
          "A json object or a json array were expected to build a document"
        )
    }

  override private[rdf] def compact(
      input: Json,
      ctx: ContextValue
  )(using RemoteContextResolution): IO[JsonObject] = {
    for {
      document  <- tryExpensiveIO(circeToDocument(input), "building input")
      context   <- tryExpensiveIO(ctx.titaniumDocument, "building context")
      options   <- documentLoader(input, ctx.contextObj.asJson).map(toOpts)
      compacted <-
        tryExpensiveIO(jakartaJsonToCirceObject(JsonLd.compact(document, context).options(options).get()), "compacting")
    } yield compacted
  }

  override private[rdf] def expand(
      input: Json
  )(using RemoteContextResolution): IO[Seq[JsonObject]] =
    explainExpand(input).map(_.value)

  override private[rdf] def explainExpand(
      input: Json
  )(using rcr: RemoteContextResolution): IO[ExplainResult[Seq[JsonObject]]] =
    for {
      document       <- tryExpensiveIO(circeToDocument(input), "building input")
      remoteContexts <- remoteContexts(input)
      options         = toOpts(TitaniumDocumentLoader(remoteContexts))
      expanded       <- tryExpensiveIO(jakartaJsonToCirce(JsonLd.expand(document).options(options).get()), "expanding")
      expandedSeqObj <- IO.fromEither(toSeqJsonObjectOrErr(expanded))
    } yield ExplainResult(remoteContexts, expandedSeqObj)

  override private[rdf] def frame(
      input: Json,
      frame: Json
  )(using RemoteContextResolution): IO[JsonObject] =
    for {
      obj     <- tryExpensiveIO(circeToDocument(input), "building input")
      ff      <- tryExpensiveIO(circeToDocument(frame), "building frame")
      options <- documentLoader(input, frame).map(toOpts)
      framed  <- tryExpensiveIO(jakartaJsonToCirceObject(JsonLd.frame(obj, ff).options(options).get), "framing")
    } yield framed

  override private[rdf] def toRdf(input: Json): IO[DatasetGraph] = {
    def toRdf: DatasetGraph = {
      val iriResolver  = IRIxResolver.create
        .base(opts.base.map(_.toString).orNull)
        .resolve(!config.strict)
        .allowRelative(!config.strict)
        .build()
      val errorHandler = config.errorHandling match {
        case ErrorHandling.Default   => ErrorHandlerFactory.getDefaultErrorHandler
        case ErrorHandling.Strict    => ErrorHandlerFactory.errorHandlerStrictNoLogging
        case ErrorHandling.NoWarning => ErrorHandlerFactory.errorHandlerNoWarnings
      }
      val profile      = new CDTAwareParserProfile(
        RiotLib.factoryRDF,
        errorHandler,
        iriResolver,
        PrefixMapFactory.create,
        RIOT.getContext.copy,
        config.extraChecks,
        config.strict
      )
      val document     = circeToDocument(input)
      val dataset      = DatasetFactory.create().asDatasetGraph()
      val output       = StreamRDFLib.dataset(dataset)
      val options      = toOpts(TitaniumDocumentLoader.empty)
      TitaniumToJena.convert(document, options, output, profile)
      dataset
    }

    tryExpensiveIO(toRdf, "toRdf")
  }

  override private[rdf] def fromRdf(
      input: DatasetGraph
  ): IO[Seq[JsonObject]] = {
    def fromRdf = {
      val opts      = toOpts(TitaniumDocumentLoader.empty)
      val jsonArray = jakartaJsonToCirce(JenaToTitanium.convert(input, opts))
      toSeqJsonObjectOrErr(jsonArray)
    }

    tryExpensiveIO(fromRdf, "fromRdf").rethrow
  }

  override private[rdf] def context(
      value: ContextValue
  )(using rcr: RemoteContextResolution): IO[JsonLdContext] =
    for {
      dl                       <- documentLoader(value.contextObj.asJson)
      opts                      = toOpts(dl)
      contextValue              = circeToJakarta(value.value)
      ctx                      <- IO.fromTry(Try(new ActiveContext(ProcessingRuntime.of(opts)).newContext.create(contextValue, null)))
                                    .adaptError { err => UnexpectedJsonLdContext(err.getMessage) }
      base                      = Option(ctx.getBaseUri).map { base => iri"$base" }
      vocab                     = Option(ctx.getVocabularyMapping).map { vocab => iri"$vocab" }
      (aliases, prefixMappings) = extractTerms(ctx)
    } yield JsonLdContext(value, base, vocab, aliases, prefixMappings)

  private def extractTerms(activeContext: ActiveContext) = {
    val init = Map.empty[String, Iri]
    activeContext.getTermsMapping.asScala.foldLeft((init, init)) { case ((aliases, prefixMappings), (key, term)) =>
      val entry = key -> iri"${term.getUriMapping}"
      if term.isPrefix then (aliases, prefixMappings + entry)
      else (aliases + entry, prefixMappings)
    }
  }

  private def remoteContexts(
      jsons: Json*
  )(using rcr: RemoteContextResolution): IO[Map[Iri, RemoteContext]] =
    jsons
      .parTraverse(rcr(_))
      .adaptError { case r: RemoteContextResolutionError => RemoteContextError(r) }
      .map(_.foldLeft(Map.empty[Iri, RemoteContext])(_ ++ _))

  private def documentLoader(jsons: Json*)(using RemoteContextResolution): IO[DocumentLoader] =
    remoteContexts(jsons*).map(TitaniumDocumentLoader(_))

  private def toOpts(dl: DocumentLoader): TitaniumJsonLdOptions = {
    val titaniumOpts = new TitaniumJsonLdOptions(dl)
    opts.base.foreach(b => titaniumOpts.setBase(new URI(b.toString)))
    titaniumOpts.setCompactArrays(opts.compactArrays)
    titaniumOpts.setCompactToRelative(opts.compactToRelative)
    titaniumOpts.setOrdered(opts.ordered)
    titaniumOpts.setProcessingMode(opts.processingMode)
    titaniumOpts.setProduceGeneralizedRdf(opts.produceGeneralizedRdf)
    opts.rdfDirection.foreach { dir => titaniumOpts.setRdfDirection(RdfDirection.valueOf(dir)) }
    titaniumOpts.setUseNativeTypes(opts.useNativeTypes)
    titaniumOpts.setUseRdfType(opts.useRdfType)
    titaniumOpts.setEmbed(opts.embed)
    titaniumOpts.setExplicit(opts.explicit)
    titaniumOpts.setOmitDefault(opts.omitDefault)
    titaniumOpts.setOmitGraph(opts.omitGraph)
    // Disabling uri validation, Jena handles it better at a later stage
    titaniumOpts.setUriValidation(UriValidationPolicy.None)
    titaniumOpts
  }

  private def toSeqJsonObjectOrErr(json: Json): Either[RdfError, Seq[JsonObject]] =
    json.asArray
      .flatMap(_.foldM(Vector.empty[JsonObject])((seq, json) => json.asObject.map(seq :+ _)))
      .toRight(UnexpectedJsonLd("Expected a sequence of Json Object"))
}

object TitaniumJsonLdApi {

  /**
    * Creates an API with a config with strict values
    */
  val strict: JsonLdApi = strict(JsonLdOptions.Defaults)

  def strict(opts: JsonLdOptions): JsonLdApi =
    new TitaniumJsonLdApi(
      JsonLdApiConfig(strict = true, extraChecks = true, errorHandling = ErrorHandling.Strict),
      opts
    )

  /**
    * Creates an API with a config with lenient values
    */
  val lenient: JsonLdApi = lenient(JsonLdOptions.Defaults)

  def lenient(opts: JsonLdOptions) = new TitaniumJsonLdApi(
    JsonLdApiConfig(strict = false, extraChecks = false, errorHandling = ErrorHandling.NoWarning),
    opts
  )

  private[rdf] def tryExpensiveIO[A](value: => A, stage: String): IO[A] =
    IO.cede *> IO.fromEither(tryOrRdfError(value, stage)).guarantee(IO.cede)

  private[rdf] def tryOrRdfError[A](value: => A, stage: String): Either[RdfError, A] =
    Try(value).toEither.leftMap {
      case err: JsonLdError =>
        val rootMessage = ExceptionUtils.getRootCauseMessage(err)
        ConversionError(rootMessage, stage)
      case err              =>
        ConversionError(err.getMessage, stage)
    }
}
