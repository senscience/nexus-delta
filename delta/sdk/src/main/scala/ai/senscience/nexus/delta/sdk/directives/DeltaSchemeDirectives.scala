package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.{idSegment, projectRef}
import ai.senscience.nexus.delta.sdk.marshalling.QueryParamsUnmarshalling
import ai.senscience.nexus.delta.sdk.marshalling.QueryParamsUnmarshalling.IriVocab
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ProjectContext
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, ProjectRef}
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.Uri.Path./
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.{Directive0, Directive1}
import cats.effect.IO
import cats.effect.unsafe.implicits.*

/**
  * Directives requiring interactions with the projects and organizations modules
  *
  * @param fetchContext
  *   fetch the context for a project
  */
final class DeltaSchemeDirectives(
    fetchContext: ProjectRef => IO[ProjectContext]
) extends QueryParamsUnmarshalling {

  def projectContext(projectRef: ProjectRef): Directive1[ProjectContext] =
    onSuccess(fetchContext(projectRef).attempt.unsafeToFuture()).flatMap {
      case Right(pc) => provide(pc)
      case Left(_)   => reject()
    }

  /**
    * Consumes a path Segment and parse it into an [[Iri]]. It fetches the project context in order to expand the
    * segment into an Iri
    */
  def iriSegment(projectRef: ProjectRef): Directive1[Iri] =
    idSegment.flatMap { idSegment =>
      onSuccess(fetchContext(projectRef).attempt.unsafeToFuture()).flatMap {
        case Right(pc) => idSegment.toIri(pc.apiMappings, pc.base).map(provide).getOrElse(reject())
        case Left(_)   => reject()
      }
    }

  /**
    * If the un-consumed request context starts by /resources/{org}/{proj}/_/{id} and it is a GET request the
    * un-consumed path it is replaced by /{rootResourceType}/{org}/{proj}/{id}
    *
    * On the other hand if the un-consumed request context starts by /resources/{org}/{proj}/{schema}/ and {schema}
    * resolves to the passed ''schemaRef'' the un-consumed path it is replaced by /{rootResourceType}/{org}/{proj}/
    *
    * Note: Use right after extracting the prefix
    */
  def replaceUri(rootResourceType: String, schemaId: Iri): Directive0 =
    replaceUriOnUnderscore(rootResourceType) & replaceUriOn(rootResourceType, schemaId)

  private def replaceUriOnUnderscore(rootResourceType: String): Directive0 =
    (pathPrefix("resources") & projectRef & pathPrefix("_") & pathPrefix(Segment) & extractMethod)
      .tflatMap { case (projectRef, id, method) =>
        mapRequestContext { ctx =>
          if (method == HttpMethods.GET || method == HttpMethods.DELETE) {
            val basePath = /(rootResourceType) / projectRef.organization.value / projectRef.project.value / id
            ctx.withUnmatchedPath(basePath ++ ctx.unmatchedPath)
          } else ctx
        }
      }
      .or(pass)

  private def replaceUriOn(rootResourceType: String, schemaId: Iri): Directive0 =
    (pathPrefix("resources") & projectRef)
      .flatMap { projectRef =>
        iriSegment(projectRef).tfilter { case Tuple1(schema) => schema == schemaId }.flatMap { _ =>
          mapRequestContext { ctx =>
            val basePath = /(rootResourceType) / projectRef.organization.value / projectRef.project.value
            ctx.withUnmatchedPath(basePath ++ ctx.unmatchedPath)
          }
        }
      }
      .or(pass)

  /**
    * Extract the ''type'' query parameter(s) as Iri
    */
  def types(implicit projectRef: ProjectRef): Directive1[IriFilter] =
    onSuccess(fetchContext(projectRef).attempt.unsafeToFuture()).flatMap {
      case Right(projectContext) =>
        implicit val pc: ProjectContext = projectContext
        parameter("type".as[IriVocab].*).map[IriFilter](t =>
          IriFilter.fromSet(t.toSet.map((iriVocab: IriVocab) => iriVocab.value))
        )
      case _                     => provide(IriFilter.None)
    }
}

object DeltaSchemeDirectives extends QueryParamsUnmarshalling {

  def apply(fetchContext: FetchContext): DeltaSchemeDirectives =
    new DeltaSchemeDirectives((ref: ProjectRef) => fetchContext.onRead(ref))

}
