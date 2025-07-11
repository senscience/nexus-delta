package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.plugins.compositeviews.client.DeltaClient.RemoteCheckError
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.{CrossProjectSource, RemoteProjectSource}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegmentRef}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import akka.http.scaladsl.model.StatusCodes
import io.circe.syntax.*
import io.circe.{Encoder, Json, JsonObject}

/**
  * Enumeration of composite view rejection types.
  *
  * @param reason
  *   a descriptive message as to why the rejection occurred
  */
sealed abstract class CompositeViewRejection(val reason: String) extends Rejection

object CompositeViewRejection {

  /**
    * Rejection returned when attempting to create a view with an id that already exists.
    *
    * @param id
    *   the view id
    */
  final case class ViewAlreadyExists(id: Iri, project: ProjectRef)
      extends CompositeViewRejection(s"Composite view '$id' already exists in project '$project'.")

  /**
    * Rejection returned when attempting to create a composite view but the id already exists for another resource type.
    *
    * @param id
    *   the resource identifier
    * @param project
    *   the project it belongs to
    */
  final case class ResourceAlreadyExists(id: Iri, project: ProjectRef)
      extends CompositeViewRejection(s"Resource '$id' already exists in project '$project'.")

  /**
    * Rejection returned when a view doesn't exist.
    *
    * @param id
    *   the view id
    */
  final case class ViewNotFound(id: Iri, project: ProjectRef)
      extends CompositeViewRejection(s"Composite view '$id' not found in project '$project'.")

  /**
    * Rejection returned when a view projection doesn't exist.
    */
  final case class ProjectionNotFound private (msg: String) extends CompositeViewRejection(msg)

  object ProjectionNotFound {

    def apply(ref: ViewRef, projectionId: Iri): ProjectionNotFound =
      apply(ref.viewId, projectionId, ref.project)

    def apply(id: Iri, projectionId: Iri, project: ProjectRef): ProjectionNotFound =
      ProjectionNotFound(s"Projection '$projectionId' not found in composite view '$id' and project '$project'.")

    def apply(ref: ViewRef, projectionId: Iri, tpe: ProjectionType): ProjectionNotFound =
      apply(ref.viewId, projectionId, ref.project, tpe)

    def apply(id: Iri, projectionId: Iri, project: ProjectRef, tpe: ProjectionType): ProjectionNotFound =
      ProjectionNotFound(s"$tpe '$projectionId' not found in composite view '$id' and project '$project'.")

  }

  /**
    * Rejection returned when a view source doesn't exist.
    */
  final case class SourceNotFound(id: Iri, projectionId: Iri, project: ProjectRef)
      extends CompositeViewRejection(
        s"Projection '$projectionId' not found in composite view '$id' and project '$project'."
      )

  object SourceNotFound {
    def apply(ref: ViewRef, projectionId: Iri): SourceNotFound =
      new SourceNotFound(ref.viewId, projectionId, ref.project)
  }

  /**
    * Rejection returned when attempting to update/deprecate a view that is already deprecated.
    *
    * @param id
    *   the view id
    */
  final case class ViewIsDeprecated(id: Iri) extends CompositeViewRejection(s"Composite view '$id' is deprecated.")

  /**
    * Rejection returned when attempting to undeprecate a view that is not deprecated.
    *
    * @param id
    *   the view id
    */
  final case class ViewIsNotDeprecated(id: Iri)
      extends CompositeViewRejection(s"Composite view '$id' is not deprecated.")

  /**
    * Rejection returned when a subject intends to perform an operation on the current view, but either provided an
    * incorrect revision or a concurrent update won over this attempt.
    *
    * @param provided
    *   the provided revision
    * @param expected
    *   the expected revision
    */
  final case class IncorrectRev(provided: Int, expected: Int)
      extends CompositeViewRejection(
        s"Incorrect revision '$provided' provided, expected '$expected', the view may have been updated since last seen."
      )

  final case class FetchByTagNotSupported(tag: IdSegmentRef.Tag)
      extends CompositeViewRejection(
        s"Fetching composite views by tag is no longer supported. Id ${tag.value.asString} and tag ${tag.tag.value}"
      )

  /**
    * Rejection returned when a subject intends to retrieve a view at a specific revision, but the provided revision
    * does not exist.
    *
    * @param provided
    *   the provided revision
    * @param current
    *   the last known revision
    */
  final case class RevisionNotFound(provided: Int, current: Int)
      extends CompositeViewRejection(
        s"Revision requested '$provided' not found, last known revision is '$current'."
      )

  /**
    * Rejection returned when too many sources are specified.
    *
    * @param provided
    *   the number of sources specified
    * @param max
    *   the maximum number of sources
    */
  final case class TooManySources(provided: Int, max: Int)
      extends CompositeViewRejection(
        s"$provided exceeds the maximum allowed number of sources($max)."
      )

  /**
    * Rejection returned when too many projections are specified.
    *
    * @param provided
    *   the number of projections specified
    * @param max
    *   the maximum number of projections
    */
  final case class TooManyProjections(provided: Int, max: Int)
      extends CompositeViewRejection(
        s"$provided exceeds the maximum allowed number of projections($max)."
      )

  /**
    * Rejection returned when there are duplicate ids in sources or projections.
    *
    * @param ids
    *   the ids provided
    */
  final case class DuplicateIds(ids: Set[Iri])
      extends CompositeViewRejection(
        s"The ids of projection or source contain a duplicate. Ids provided: ${ids.mkString("'", "', '", "'")}"
      )

  /**
    * Rejection signalling that a source is invalid.
    */
  sealed abstract class CompositeViewSourceRejection(reason: String) extends CompositeViewRejection(reason)

  /**
    * Rejection returned when the project for a [[CrossProjectSource]] does not exist.
    */
  final case class CrossProjectSourceProjectNotFound(crossProjectSource: CrossProjectSource)
      extends CompositeViewSourceRejection(
        s"Project ${crossProjectSource.project} does not exist for 'CrossProjectSource' ${crossProjectSource.id}"
      )

  /**
    * Rejection returned when the identities for a [[CrossProjectSource]] don't have access to target project.
    */
  final case class CrossProjectSourceForbidden(crossProjectSource: CrossProjectSource)(implicit val baseUri: BaseUri)
      extends CompositeViewSourceRejection(
        s"None of the identities  ${crossProjectSource.identities.map(_.asIri).mkString(",")} has permissions for project ${crossProjectSource.project}"
      )

  /**
    * Rejection returned when [[RemoteProjectSource]] is invalid.
    */
  final case class InvalidRemoteProjectSource(remoteProjectSource: RemoteProjectSource, error: RemoteCheckError)
      extends CompositeViewSourceRejection(
        s"RemoteProjectSource ${remoteProjectSource.tpe} is invalid: either provided endpoint '${remoteProjectSource.endpoint}' is invalid or there are insufficient permissions to access this endpoint. "
      )

  /**
    * Rejection signalling that a projection is invalid.
    */
  sealed abstract class CompositeViewProjectionRejection(reason: String) extends CompositeViewRejection(reason)

  /**
    * Rejection returned when the provided ElasticSearch mapping for an ElasticSearchProjection is invalid.
    */
  final case class InvalidElasticSearchProjectionPayload(details: Option[Json])
      extends CompositeViewProjectionRejection(
        "The provided ElasticSearch mapping value is invalid."
      )

  /**
    * Signals a rejection caused by an attempt to create or update an composite view with a permission that is not
    * defined in the permission set singleton.
    *
    * @param permission
    *   the provided permission
    */
  final case class PermissionIsNotDefined(permission: Permission)
      extends CompositeViewProjectionRejection(
        s"The provided permission '${permission.value}' is not defined in the collection of allowed permissions."
      )

  /**
    * Rejection returned when attempting to interact with a composite while providing an id that cannot be resolved to
    * an Iri.
    *
    * @param id
    *   the view identifier
    */
  final case class InvalidCompositeViewId(id: String)
      extends CompositeViewRejection(s"Composite view identifier '$id' cannot be expanded to an Iri.")

  implicit private[plugins] val compositeViewRejectionEncoder: Encoder.AsObject[CompositeViewRejection] =
    Encoder.AsObject.instance { r =>
      val tpe = ClassUtils.simpleName(r)
      val obj = JsonObject(keywords.tpe -> tpe.asJson, "reason" -> r.reason.asJson)
      r match {
        case IncorrectRev(provided, expected)               => obj.add("provided", provided.asJson).add("expected", expected.asJson)
        case InvalidElasticSearchProjectionPayload(details) => obj.addIfExists("details", details)
        case InvalidRemoteProjectSource(_, error)           =>
          obj.add("details", error.reason.asJson).add("responseBody", error.body.getOrElse(Json.Null))
        case _: ViewNotFound                                => obj.add(keywords.tpe, "ResourceNotFound".asJson)
        case _                                              => obj
      }
    }

  implicit final val compositeViewRejectionJsonLdEncoder: JsonLdEncoder[CompositeViewRejection] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.error))

  implicit val compositeViewHttpResponseFields: HttpResponseFields[CompositeViewRejection] =
    HttpResponseFields {
      case RevisionNotFound(_, _)      => StatusCodes.NotFound
      case ViewNotFound(_, _)          => StatusCodes.NotFound
      case ProjectionNotFound(_)       => StatusCodes.NotFound
      case SourceNotFound(_, _, _)     => StatusCodes.NotFound
      case ViewAlreadyExists(_, _)     => StatusCodes.Conflict
      case ResourceAlreadyExists(_, _) => StatusCodes.Conflict
      case IncorrectRev(_, _)          => StatusCodes.Conflict
      case _                           => StatusCodes.BadRequest
    }
}
