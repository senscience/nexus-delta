package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, IndexLabel}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.PermissionIsNotDefined
import ai.senscience.nexus.delta.elasticsearch.query.ElasticSearchClientError.ElasticsearchCreateIndexError
import ai.senscience.nexus.delta.plugins.compositeviews.client.DeltaClient
import ai.senscience.nexus.delta.plugins.compositeviews.client.DeltaClient.RemoteCheckError
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.projectionIndex
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.{ElasticSearchProjection, SparqlProjection}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.{CrossProjectSourceForbidden, CrossProjectSourceProjectNotFound, DuplicateIds, InvalidElasticSearchProjectionPayload, InvalidRemoteProjectSource, TooManyProjections, TooManySources}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.{CrossProjectSource, ProjectSource, RemoteProjectSource}
import ai.senscience.nexus.delta.plugins.compositeviews.model.{CompositeViewProjection, CompositeViewSource, CompositeViewValue}
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.events
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.Projects
import cats.effect.IO
import cats.syntax.all.*

import java.util.UUID

/**
  * Validate an [[CompositeViewValue]] during command evaluation
  */
trait ValidateCompositeView {

  def apply(uuid: UUID, value: CompositeViewValue): IO[Unit]

}

object ValidateCompositeView {

  def apply(
      aclCheck: AclCheck,
      projects: Projects,
      fetchPermissions: IO[Set[Permission]],
      client: ElasticSearchClient,
      deltaClient: DeltaClient,
      prefix: String,
      maxSources: Int,
      maxProjections: Int
  )(implicit baseUri: BaseUri): ValidateCompositeView = (uuid: UUID, value: CompositeViewValue) => {
    def validateAcls(cpSource: CrossProjectSource): IO[Unit] =
      aclCheck.authorizeForOr(cpSource.project, events.read, cpSource.identities)(CrossProjectSourceForbidden(cpSource))

    def validateProject(cpSource: CrossProjectSource) =
      projects.fetch(cpSource.project).orRaise(CrossProjectSourceProjectNotFound(cpSource)).void

    def validatePermission(permission: Permission) =
      fetchPermissions.flatMap { perms =>
        IO.raiseWhen(!perms.contains(permission))(PermissionIsNotDefined(permission))
      }

    def validateIndex(es: ElasticSearchProjection, index: IndexLabel) =
      client
        .createIndex(index, Some(es.mapping), es.settings)
        .adaptError { case err: ElasticsearchCreateIndexError => InvalidElasticSearchProjectionPayload(err.body) }
        .void

    val checkRemoteEvent: RemoteProjectSource => IO[Unit] = deltaClient.checkElems(_)

    val validateSource: CompositeViewSource => IO[Unit] = {
      case _: ProjectSource             => IO.unit
      case cpSource: CrossProjectSource => validateAcls(cpSource) >> validateProject(cpSource)
      case rs: RemoteProjectSource      =>
        checkRemoteEvent(rs).adaptError { case e: RemoteCheckError => InvalidRemoteProjectSource(rs, e) }
    }

    val validateProjection: CompositeViewProjection => IO[Unit] = {
      case sparql: SparqlProjection    => validatePermission(sparql.permission)
      case es: ElasticSearchProjection =>
        validatePermission(es.permission) >>
          validateIndex(es, projectionIndex(es, uuid, prefix))
    }

    for {
      _          <- IO.raiseWhen(value.sources.length > maxSources)(TooManySources(value.sources.length, maxSources))
      _          <- IO.raiseWhen(value.projections.length > maxProjections)(
                      TooManyProjections(value.projections.length, maxProjections)
                    )
      idIntersect = value.sources.keys.intersect(value.projections.keys)
      _          <- IO.raiseWhen(idIntersect.nonEmpty)(DuplicateIds(idIntersect))
      _          <- value.sources.toNel.foldLeftM(()) { case (_, (_, s)) => validateSource(s) }
      _          <- value.projections.toNel.foldLeftM(()) { case (_, (_, p)) => validateProjection(p) }

    } yield ()

  }

}
