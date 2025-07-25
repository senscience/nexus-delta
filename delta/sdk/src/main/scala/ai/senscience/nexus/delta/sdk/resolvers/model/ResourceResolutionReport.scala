package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.resolvers.model.ResourceResolutionReport.ResolverReport
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder
import io.circe.syntax.*
import io.circe.{Encoder, Json, JsonObject}

import scala.collection.immutable.VectorMap

/**
  * Report describing how the resource resolution went for each resolver
  * @param history
  *   how the resolution went
  */
final case class ResourceResolutionReport(history: Vector[ResolverReport])

object ResourceResolutionReport {

  /**
    * Creates a [[ResourceResolutionReport]]
    * @param history
    *   the different reports for each resolver
    */
  def apply(history: ResolverReport*): ResourceResolutionReport = new ResourceResolutionReport(Vector.from(history))

  /**
    * Subreport describing how the resolution went for a single resolver
    */
  sealed trait ResolverReport extends Product with Serializable {

    /**
      * @return
      *   the resolver
      */
    def resolverId: Iri

    /**
      * @return
      *   Causes of the failed attempts to resolve a resource with this resolver
      */
    def rejections: VectorMap[ProjectRef, ResolverResolutionRejection]

    def success: Boolean
  }

  object ResolverReport {

    /**
      * Create a [[ResolverSuccessReport]]
      * @param resolverId
      *   the resolver
      * @param resourceProject
      *   the project where the resource has been resolved
      * @param rejections
      *   the eventual rejections
      * @return
      */
    def success(
        resolverId: Iri,
        resourceProject: ProjectRef,
        rejections: (ProjectRef, ResolverResolutionRejection)*
    ): ResolverSuccessReport =
      ResolverSuccessReport(resolverId, resourceProject, VectorMap.from(rejections))

    /**
      * Create a [[ResolverFailedReport]]
      * @param resolverId
      *   the resolver
      * @param first
      *   the mandatory first rejection
      * @param others
      *   other rejections that may have happened for other projects
      * @return
      */
    def failed(
        resolverId: Iri,
        first: (ProjectRef, ResolverResolutionRejection),
        others: (ProjectRef, ResolverResolutionRejection)*
    ): ResolverFailedReport =
      ResolverFailedReport(resolverId, VectorMap(first) ++ others)

  }

  /**
    * Report failures for a single resolver
    */
  final case class ResolverFailedReport(resolverId: Iri, rejections: VectorMap[ProjectRef, ResolverResolutionRejection])
      extends ResolverReport {
    override def success: Boolean = false
  }

  /**
    * Report success for a single resolver with previous attempts in case of a cross-project resolver
    */
  final case class ResolverSuccessReport(
      resolverId: Iri,
      resourceProject: ProjectRef,
      rejections: VectorMap[ProjectRef, ResolverResolutionRejection]
  ) extends ResolverReport {
    override def success: Boolean = true
  }

  implicit private val config: Configuration = Configuration.default

  implicit val resolverReportEncoder: Encoder.AsObject[ResolverReport] = {
    Encoder.AsObject.instance { r =>
      JsonObject(
        "resolverId" -> r.resolverId.asJson,
        "success"    -> r.success.asJson,
        "rejections" -> Json.fromValues(
          r.rejections.map { case (project, rejection) =>
            Json.obj("project" -> project.asJson, "cause" -> rejection.asJson)
          }
        )
      ).deepMerge(
        r match {
          case _: ResolverFailedReport                      => JsonObject.empty
          case ResolverSuccessReport(_, resourceProject, _) =>
            JsonObject(
              "resourceProject" -> resourceProject.asJson
            )
        }
      )
    }
  }

  implicit val resourceResolutionReportEncoder: Encoder.AsObject[ResourceResolutionReport] =
    deriveConfiguredEncoder[ResourceResolutionReport]

  implicit final val resolverReportJsonLdEncoder: JsonLdEncoder[ResolverReport] =
    JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.resolvers))

  implicit val resolverReportHttpResponseFields: HttpResponseFields[ResolverReport] = HttpResponseFields.defaultOk

  implicit final val resourceResolutionReportJsonLdEncoder: JsonLdEncoder[ResourceResolutionReport] =
    JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.resolvers))

  implicit val resourceResolutionReportHttpResponseFields: HttpResponseFields[ResourceResolutionReport] =
    HttpResponseFields.defaultOk
}
