package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.syntax.jsonObjectOpsSyntax
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.resolvers.model.ResourceResolutionReport.ResolverReport
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.{Latest, Revision, Tag}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef, ResourceRef}
import akka.http.scaladsl.model.StatusCodes
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}

/**
  * Enumeration of Resolver rejection types.
  *
  * @param reason
  *   a descriptive message as to why the rejection occurred
  */
sealed abstract class ResolverRejection(val reason: String) extends Rejection

object ResolverRejection {

  /**
    * Rejection returned when a subject intends to retrieve a resolver at a specific revision, but the provided revision
    * does not exist.
    *
    * @param provided
    *   the provided revision
    * @param current
    *   the last known revision
    */
  final case class RevisionNotFound(provided: Int, current: Int)
      extends ResolverRejection(s"Revision requested '$provided' not found, last known revision is '$current'.")

  /**
    * Rejection returned when attempting to fetch a resolver by tag, a feature no longer supporter
    */
  final case class FetchByTagNotSupported(tag: UserTag)
      extends ResolverRejection(
        s"Fetching resolvers by tag is no longer supported. Id ${tag.value} and tag ${tag.value}"
      )

  /**
    * Rejection returned when attempting to create a resolver but the id already exists.
    *
    * @param id
    *   the resource identifier
    * @param project
    *   the project it belongs to
    */
  final case class ResourceAlreadyExists(id: Iri, project: ProjectRef)
      extends ResolverRejection(s"Resource '$id' already exists in project '$project'.")

  /**
    * Rejection returned when attempting to update a resolver with an id that doesn't exist.
    *
    * @param id
    *   the resolver identifier
    * @param project
    *   the project it belongs to
    */
  final case class ResolverNotFound(id: Iri, project: ProjectRef)
      extends ResolverRejection(s"Resolver '$id' not found in project '$project'.")

  /**
    * Rejection returned when attempting to interact with a resolver providing an id that cannot be resolved to an Iri.
    *
    * @param id
    *   the resolver identifier
    */
  final case class InvalidResolverId(id: String)
      extends ResolverRejection(s"Resolver identifier '$id' cannot be expanded to an Iri.")

  /**
    * Rejection returned when attempting to resolve a resource providing an id that cannot be resolved to an Iri.
    *
    * @param id
    *   the resolver identifier
    */
  final case class InvalidResolvedResourceId(id: String)
      extends ResolverRejection(s"Resource identifier '$id' cannot be expanded to an Iri.")

  /**
    * Signals an error when there is another resolver with the provided priority already existing
    */
  final case class PriorityAlreadyExists(project: ProjectRef, id: Iri, priority: Priority)
      extends ResolverRejection(s"Resolver '$id' in project '$project' already has the provided priority '$priority'.")

  /**
    * Rejection returned when attempting to create a resolver with an id that already exists.
    *
    * @param id
    *   the resolver identifier
    */
  final case class DifferentResolverType(id: Iri, found: ResolverType, expected: ResolverType)
      extends ResolverRejection(s"Resolver '$id' is of type '$expected' and can't be updated to be a '$found' .")

  /**
    * Rejection returned when no identities has been provided
    */
  final case object NoIdentities extends ResolverRejection(s"At least one identity of the caller must be provided.")

  /**
    * Rejection return when the logged caller does not have one of the provided identities
    */
  final case class InvalidIdentities(missingIdentities: Set[Identity])
      extends ResolverRejection(
        s"The caller doesn't have some of the provided identities: ${missingIdentities.mkString(",")}"
      )

  /**
    * Rejection returned when a subject intends to perform an operation on the current resolver, but either provided an
    * incorrect revision or a concurrent update won over this attempt.
    *
    * @param provided
    *   the provided revision
    * @param expected
    *   the expected revision
    */
  final case class IncorrectRev(provided: Int, expected: Int)
      extends ResolverRejection(
        s"Incorrect revision '$provided' provided, expected '$expected', the resolver may have been updated since last seen."
      )

  private def formatResourceRef(resourceRef: ResourceRef) =
    resourceRef match {
      case Latest(iri)           => s"'$iri' (latest)"
      case Revision(_, iri, rev) => s"'$iri' (revision: $rev)"
      case Tag(_, iri, tag)      => s"'$iri' (tag: $tag)"
    }

  /**
    * Rejection returned when attempting to resolve a resourceId as a data resource or as schema using all resolvers of
    * the given project
    * @param resourceRef
    *   the resource reference to resolve
    * @param projectRef
    *   the project where we want to resolve from
    * @param report
    *   the report for the resolution attempt
    */
  final case class InvalidResolution(
      resourceRef: ResourceRef,
      projectRef: ProjectRef,
      report: ResourceResolutionReport
  ) extends ResolverRejection(
        s"Failed to resolve ${formatResourceRef(resourceRef)} using resolvers of project '$projectRef'."
      )

  /**
    * Rejection returned when attempting to resolve a resourceId as a data resource or as schema using the specified
    * resolver id
    * @param resourceRef
    *   the resource reference to resolve
    * @param resolverId
    *   the id of the resolver
    * @param projectRef
    *   the project where we want to resolve from
    * @param report
    *   the report for resolution attempt
    */
  final case class InvalidResolverResolution(
      resourceRef: ResourceRef,
      resolverId: Iri,
      projectRef: ProjectRef,
      report: ResolverReport
  ) extends ResolverRejection(
        s"Failed to resolve ${formatResourceRef(resourceRef)} using resolver '$resolverId' of project '$projectRef'."
      )

  /**
    * Rejection returned when attempting to update/deprecate a resolver that is already deprecated.
    *
    * @param id
    *   the resolver identifier
    */
  final case class ResolverIsDeprecated(id: Iri) extends ResolverRejection(s"Resolver '$id' is deprecated.")

  implicit val resolverRejectionEncoder: Encoder.AsObject[ResolverRejection] =
    Encoder.AsObject.instance { r =>
      val tpe = ClassUtils.simpleName(r)
      val obj = JsonObject.empty.add(keywords.tpe, tpe.asJson).add("reason", r.reason.asJson)
      r match {
        case IncorrectRev(provided, expected)           => obj.add("provided", provided.asJson).add("expected", expected.asJson)
        case InvalidResolution(_, _, report)            => obj.addContext(contexts.resolvers).add("report", report.asJson)
        case InvalidResolverResolution(_, _, _, report) =>
          obj.addContext(contexts.resolvers).add("report", report.asJson)
        case _: ResolverNotFound                        => obj.add(keywords.tpe, "ResourceNotFound".asJson)
        case _                                          => obj
      }
    }

  implicit final val resourceRejectionJsonLdEncoder: JsonLdEncoder[ResolverRejection] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.error))

  implicit val responseFieldsResolvers: HttpResponseFields[ResolverRejection] =
    HttpResponseFields {
      case RevisionNotFound(_, _)                => StatusCodes.NotFound
      case ResolverNotFound(_, _)                => StatusCodes.NotFound
      case InvalidResolution(_, _, _)            => StatusCodes.NotFound
      case InvalidResolverResolution(_, _, _, _) => StatusCodes.NotFound
      case ResourceAlreadyExists(_, _)           => StatusCodes.Conflict
      case IncorrectRev(_, _)                    => StatusCodes.Conflict
      case _                                     => StatusCodes.BadRequest
    }
}
