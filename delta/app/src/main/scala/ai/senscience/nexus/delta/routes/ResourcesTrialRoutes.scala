package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.Vocabulary.schemas
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ResourcesTrialRoutes.SchemaInput.{ExistingSchema, NewSchema}
import ai.senscience.nexus.delta.routes.ResourcesTrialRoutes.{GenerateSchema, GenerationInput}
import ai.senscience.nexus.delta.sdk.SchemaResource
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.IdSegment.IriSegment
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.write as Write
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection
import ai.senscience.nexus.delta.sdk.resources.{NexusSource, ResourcesTrial}
import ai.senscience.nexus.delta.sdk.schemas.Schemas
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaRejection
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.circe.{Decoder, Json}
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}
import org.typelevel.otel4s.trace.Tracer

/**
  * The resource trial routes allowing to do read-only operations on resources
  */
final class ResourcesTrialRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    generateSchema: GenerateSchema,
    resourcesTrial: ResourcesTrial
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  private val exceptionHandler =
    handleExceptions {
      ExceptionHandler {
        case err: ResourceRejection => discardEntityAndForceEmit(err)
        case err: SchemaRejection   => discardEntityAndForceEmit(err)
      }
    }

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & exceptionHandler) {
      concat(validateRoute, generateRoute)
    }

  private def validateRoute: Route =
    pathPrefix("resources") {
      extractCaller { implicit caller =>
        projectRef.apply { project =>
          (idSegment & idSegmentRef & pathPrefix("validate") & pathEndOrSingleSlash & get) { (schema, id) =>
            authorizeFor(project, Write).apply {
              val schemaOpt = underscoreToOption(schema)
              emit(resourcesTrial.validate(id, project, schemaOpt))
            }
          }
        }
      }
    }

  private def generateRoute: Route =
    (pathPrefix("trial") & pathPrefix("resources") & post) {
      extractCaller { implicit caller =>
        (projectRef & pathEndOrSingleSlash) { project =>
          authorizeFor(project, Write).apply {
            entity(as[GenerationInput]) { input =>
              generate(project, input)
            }
          }
        }
      }
    }

  // Call the generate method matching the schema input
  private def generate(project: ProjectRef, input: GenerationInput)(implicit caller: Caller) =
    input.schema match {
      case ExistingSchema(schemaId) =>
        emit(
          resourcesTrial
            .generate(project, schemaId, input.resource)
            .flatMap(_.asJson)
        )
      case NewSchema(schemaSource)  =>
        emit(
          generateSchema(project, schemaSource, caller)
            .flatMap { schema =>
              resourcesTrial
                .generate(project, schema, input.resource)
                .flatMap(_.asJson)
            }
        )
    }
}

object ResourcesTrialRoutes {

  type GenerateSchema = (ProjectRef, Json, Caller) => IO[SchemaResource]

  sealed private[routes] trait SchemaInput extends Product

  private[routes] object SchemaInput {

    // Validate the generated resource with an existing schema
    final case class ExistingSchema(id: IdSegment) extends SchemaInput

    // Validate the generated resource with the new schema bundled in the request
    final case class NewSchema(json: Json) extends SchemaInput

    implicit val schemaInputDecoder: Decoder[SchemaInput] =
      Decoder.instance { hc =>
        val value          = hc.value
        val existingSchema = value.asString.map { s => ExistingSchema(IdSegment(s)) }
        val newSchema      = NewSchema(value)
        Right(existingSchema.getOrElse(newSchema))
      }
  }

  private val noSchema = ExistingSchema(IriSegment(schemas.resources))

  final private[routes] case class GenerationInput(schema: SchemaInput = noSchema, resource: NexusSource)

  private[routes] object GenerationInput {

    implicit val generationInputDecoder: Decoder[GenerationInput] = {
      implicit val configuration: Configuration = Configuration.default.withDefaults
      deriveConfiguredDecoder[GenerationInput]
    }
  }

  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      schemas: Schemas,
      resourcesTrial: ResourcesTrial
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): ResourcesTrialRoutes =
    new ResourcesTrialRoutes(
      identities,
      aclCheck,
      (project, source, caller) => schemas.createDryRun(project, source)(caller),
      resourcesTrial
    )
}
