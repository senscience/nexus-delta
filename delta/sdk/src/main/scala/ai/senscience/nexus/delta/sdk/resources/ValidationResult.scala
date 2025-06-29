package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.shacl.ValidationReport
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Encoder, JsonObject}

/**
  * Result of resource validation performed by [[ValidateResource]]
  */
sealed trait ValidationResult extends Product with Serializable {

  /**
    * The schema used during validation
    */
  def schema: ResourceRef.Revision

  /**
    * The project containing this schema
    */
  def project: ProjectRef
}

object ValidationResult {

  /**
    * When the schema in unconstrained, no schema validation is performed
    * @param project
    *   the project of the resource
    */
  final case class NoValidation(project: ProjectRef) extends ValidationResult {
    override def schema: ResourceRef.Revision = ResourceRef.Revision(schemas.resources, 1)
  }

  /**
    * When a schema is specified and validation is a success, returns a report
    * @param project
    *   the project of the schema
    * @param schema
    *   its reference
    * @param report
    *   the shacl report
    */
  final case class Validated(project: ProjectRef, schema: ResourceRef.Revision, report: ValidationReport)
      extends ValidationResult

  implicit val resourceRejectionEncoder: Encoder.AsObject[ValidationResult] =
    Encoder.AsObject.instance[ValidationResult] { r =>
      val tpe = ClassUtils.simpleName(r)
      val obj = JsonObject(
        keywords.tpe := tpe,
        "schema"     := r.schema,
        "project"    := r.project
      )
      r match {
        case NoValidation(_)         => obj
        case Validated(_, _, report) =>
          obj
            .add("report", report.asJson)
            .add("@context", "https://bluebrain.github.io/nexus/contexts/shacl-20170720.json".asJson)
      }
    }

  implicit val resourceRejectionJsonLdEncoder: JsonLdEncoder[ValidationResult] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.validation))

  implicit val validationResultHttpResponseFields: HttpResponseFields[ValidationResult] = HttpResponseFields.defaultOk
}
