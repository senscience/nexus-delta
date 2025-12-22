package ai.senscience.nexus.delta.plugins.search.model

import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.config.*
import ai.senscience.nexus.delta.kernel.utils.FileUtils
import ai.senscience.nexus.delta.kernel.utils.FileUtils.loadJsonAs
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeView.{Interval, RebuildStrategy}
import ai.senscience.nexus.delta.plugins.compositeviews.model.TemplateSparqlConstructQuery
import ai.senscience.nexus.delta.plugins.search.contexts
import ai.senscience.nexus.delta.plugins.search.model.SearchConfig.IndexingConfig
import ai.senscience.nexus.delta.plugins.search.model.SearchConfigError.{InvalidRebuildStrategy, InvalidSparqlConstructQuery, InvalidSuites}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.Defaults
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef}
import cats.effect.IO
import cats.syntax.all.*
import com.typesafe.config.Config
import fs2.io.file.Path
import io.circe.syntax.KeyOps
import io.circe.{Encoder, JsonObject}
import pureconfig.{ConfigReader, ConfigSource}

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

final case class SearchConfig(
    indexing: IndexingConfig,
    fields: Option[JsonObject],
    defaults: Defaults,
    suites: SearchConfig.Suites
)

object SearchConfig {

  type Suite  = Set[ProjectRef]
  type Suites = Map[Label, Suite]

  case class NamedSuite(name: Label, suite: Suite)
  private given ConfigReader[Suites] = Label.labelMapReader[Suite]

  given Encoder[NamedSuite]       = Encoder[JsonObject].contramap(s => JsonObject("projects" := s.suite, "name" := s.name))
  given JsonLdEncoder[NamedSuite] = JsonLdEncoder.computeFromCirce(ContextValue(contexts.suites))

  given HttpResponseFields[NamedSuite] = HttpResponseFields.defaultOk

  /**
    * Converts a [[Config]] into an [[SearchConfig]]
    */
  def load(config: Config): IO[SearchConfig] = {
    val pluginConfig = config.getConfig("plugins.search")
    def loadSuites   = {
      val suiteSource = ConfigSource.fromConfig(pluginConfig).at("suites")
      IO.fromEither(suiteSource.load[Suites].leftMap(InvalidSuites(_)))
    }
    for {
      fields        <- pluginConfig.getOptionalFilePath("fields").traverse(loadJsonAs[JsonObject])
      resourceTypes <- loadJsonAs[IriFilter](pluginConfig.getFilePath("indexing.resource-types"))
      indexDef      <- ElasticsearchIndexDef.fromExternalFiles(
                         pluginConfig.getFilePath("indexing.mapping"),
                         pluginConfig.getOptionalFilePath("indexing.settings")
                       )
      query         <- loadSparqlQuery(pluginConfig.getFilePath("indexing.query"))
      context       <- pluginConfig.getOptionalFilePath("indexing.context").traverse(loadJsonAs[JsonObject])
      rebuild       <- loadRebuildStrategy(pluginConfig)
      defaults      <- loadDefaults(pluginConfig)
      suites        <- loadSuites
    } yield SearchConfig(
      IndexingConfig(
        resourceTypes,
        indexDef,
        query = query,
        context = ContextObject(context.getOrElse(JsonObject.empty)),
        rebuildStrategy = rebuild
      ),
      fields,
      defaults,
      suites
    )
  }

  private def loadSparqlQuery(filePath: Path): IO[SparqlConstructQuery] =
    for {
      content <- FileUtils.loadAsString(filePath)
      json    <- IO.fromEither(TemplateSparqlConstructQuery(content).leftMap { e =>
                   InvalidSparqlConstructQuery(filePath, e)
                 })
    } yield json

  private def loadDefaults(config: Config): IO[Defaults] =
    IO.pure(ConfigSource.fromConfig(config).at("defaults").loadOrThrow[Defaults])

  /**
    * Load the rebuild strategy from the search config. If either of the required fields is null, missing, or not a
    * correct finite duration, there will be no rebuild strategy. If both finite durations are present, then the
    * specified rebuild strategy must be greater or equal to the min rebuild interval.
    */
  private def loadRebuildStrategy(config: Config): IO[Option[RebuildStrategy]] =
    (
      readFiniteDuration(config, "indexing.rebuild-strategy"),
      readFiniteDuration(config, "indexing.min-interval-rebuild")
    ).traverseN { case (rebuild, minIntervalRebuild) =>
      IO.raiseWhen(rebuild lt minIntervalRebuild)(InvalidRebuildStrategy(rebuild, minIntervalRebuild)) >>
        IO.pure(Interval(rebuild))
    }

  private def readFiniteDuration(config: Config, path: String): Option[FiniteDuration] =
    Try(
      ConfigSource.fromConfig(config).at(path).loadOrThrow[FiniteDuration]
    ).toOption

  final case class IndexingConfig(
      resourceTypes: IriFilter,
      indexDef: ElasticsearchIndexDef,
      query: SparqlConstructQuery,
      context: ContextObject,
      rebuildStrategy: Option[RebuildStrategy]
  )

}
