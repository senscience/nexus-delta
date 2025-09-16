package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, IndexLabel}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.{InvalidElasticSearchIndexPayload, InvalidPipeline, InvalidViewReferences, PermissionIsNotDefined, TooManyViewReferences}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.{AggregateElasticSearchViewValue, IndexingElasticSearchViewValue}
import ai.senscience.nexus.delta.elasticsearch.query.ElasticSearchClientError.ElasticsearchCreateIndexError
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ValidateAggregate}
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.stream.PipeChainCompiler
import cats.effect.IO
import cats.syntax.all.*
import io.circe.JsonObject

import java.util.UUID

/**
  * Validate an [[ElasticSearchViewValue]] during command evaluation
  */
trait ValidateElasticSearchView {

  def apply(uuid: UUID, indexingRev: IndexingRev, v: ElasticSearchViewValue): IO[Unit]
}

object ValidateElasticSearchView {

  val always: ValidateElasticSearchView = (_: UUID, _: IndexingRev, _: ElasticSearchViewValue) => IO.unit

  def apply(
      pipeChainCompiler: PipeChainCompiler,
      permissions: Permissions,
      client: ElasticSearchClient,
      prefix: String,
      maxViewRefs: Int,
      xas: Transactors,
      defaultViewDef: DefaultIndexDef
  ): ValidateElasticSearchView =
    apply(
      pipeChainCompiler,
      permissions.fetchPermissionSet,
      client.createIndex(_, _, _).void,
      prefix,
      maxViewRefs,
      xas,
      defaultViewDef
    )

  def apply(
      pipeChainCompiler: PipeChainCompiler,
      fetchPermissionSet: IO[Set[Permission]],
      createIndex: (IndexLabel, Option[JsonObject], Option[JsonObject]) => IO[Unit],
      prefix: String,
      maxViewRefs: Int,
      xas: Transactors,
      defaultViewDef: DefaultIndexDef
  ): ValidateElasticSearchView = new ValidateElasticSearchView {

    private val validateAggregate = ValidateAggregate(
      ElasticSearchViews.entityType,
      InvalidViewReferences(_),
      maxViewRefs,
      TooManyViewReferences(_, _),
      xas
    )

    private def validateIndexing(uuid: UUID, indexingRev: IndexingRev, value: IndexingElasticSearchViewValue) =
      for {
        _ <- fetchPermissionSet.flatMap { set =>
               IO.raiseUnless(set.contains(value.permission))(PermissionIsNotDefined(value.permission))
             }
        _ <- IO.fromEither(value.pipeChain.traverse(pipeChainCompiler(_)).leftMap(InvalidPipeline(_)))
        _ <- createIndex(
               IndexLabel.fromView(prefix, uuid, indexingRev),
               value.mapping.orElse(Some(defaultViewDef.mapping)),
               value.settings.orElse(Some(defaultViewDef.settings))
             ).adaptError { case err: ElasticsearchCreateIndexError =>
               InvalidElasticSearchIndexPayload(err.body)
             }
      } yield ()

    override def apply(
        uuid: UUID,
        indexingRev: IndexingRev,
        value: ElasticSearchViewValue
    ): IO[Unit] = value match {
      case v: AggregateElasticSearchViewValue => validateAggregate(v.views)
      case v: IndexingElasticSearchViewValue  => validateIndexing(uuid, indexingRev, v)
    }

  }

  val alwaysValidate: ValidateElasticSearchView =
    (_: UUID, _: IndexingRev, _: ElasticSearchViewValue) => IO.unit

}
