package ai.senscience.nexus.delta.plugins.blazegraph.model

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlResults.Binding
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json, JsonObject}

import java.time.Instant
import scala.util.Try

sealed trait SparqlLink {

  /**
    * @return
    *   the @id value of the resource
    */
  def id: Iri

  /**
    * @return
    *   the collection of types of this resource
    */
  def types: Set[Iri]

  /**
    * @return
    *   the paths from where the link has been found
    */
  def paths: List[Iri]
}

object SparqlLink {

  /**
    * A link that represents a managed resource on the platform.
    */
  final case class SparqlResourceLink(
      resource: ResourceF[List[Iri]]
  ) extends SparqlLink {

    override def id: Iri = resource.id

    override def types: Set[Iri] = resource.types

    override def paths: List[Iri] = resource.value
  }

  object SparqlResourceLink {

    private def resourceUrisFor(project: ProjectRef, id: Iri): ResourceAccess = ResourceAccess.resource(project, id)

    /**
      * Attempts to create a [[SparqlResourceLink]] from the given bindings
      *
      * @param bindings
      *   the sparql result bindings
      */
    def apply(bindings: Map[String, Binding])(implicit base: BaseUri): Option[SparqlLink] =
      for {
        link         <- SparqlExternalLink(bindings)
        project      <- bindings.get(nxv.project.prefix).map(_.value).flatMap(ProjectRef.parse(_).toOption)
        rev          <- bindings.get(nxv.rev.prefix).map(_.value).flatMap(v => v.toIntOption)
        deprecated   <- bindings.get(nxv.deprecated.prefix).map(_.value).flatMap(v => v.toBooleanOption)
        created      <- bindings.get(nxv.createdAt.prefix).map(_.value).flatMap(v => Try(Instant.parse(v)).toOption)
        updated      <- bindings.get(nxv.updatedAt.prefix).map(_.value).flatMap(v => Try(Instant.parse(v)).toOption)
        createdByIri <- bindings.get(nxv.createdBy.prefix).map(_.value).flatMap(Iri.reference(_).toOption)
        createdBy    <- createdByIri.as[Subject].toOption
        updatedByIri <- bindings.get(nxv.updatedBy.prefix).map(_.value).flatMap(Iri.reference(_).toOption)
        updatedBy    <- updatedByIri.as[Subject].toOption
        schema       <- bindings.get(nxv.constrainedBy.prefix).map(_.value).flatMap(Iri.reference(_).toOption)
        schemaRef     = ResourceRef(schema)
        resourceUris  = resourceUrisFor(project, link.id)
      } yield SparqlResourceLink(
        ResourceF(
          link.id,
          resourceUris,
          rev,
          link.types,
          deprecated,
          created,
          createdBy,
          updated,
          updatedBy,
          schemaRef,
          link.paths
        )
      )

  }

  /**
    * A link that represents an external resource out of the platform.
    *
    * @param id
    *   the @id value of the resource
    * @param paths
    *   the predicate from where the link has been found
    * @param types
    *   the collection of types of this resource
    */
  final case class SparqlExternalLink(id: Iri, paths: List[Iri], types: Set[Iri] = Set.empty) extends SparqlLink

  object SparqlExternalLink {

    /**
      * Attempts to create a [[SparqlExternalLink]] from the given bindings
      *
      * @param bindings
      *   the sparql result bindings
      */
    def apply(bindings: Map[String, Binding]): Option[SparqlExternalLink] = {
      val types = bindings.get("types").map(binding => toIris(binding.value).toSet).getOrElse(Set.empty)
      val paths = bindings.get("paths").map(binding => toIris(binding.value).toList).getOrElse(List.empty)
      bindings.get("s").map(_.value).flatMap(Iri.reference(_).toOption).map(SparqlExternalLink(_, paths, types))
    }
  }

  private def toIris(string: String): Array[Iri] =
    string.split(" ").flatMap(Iri.reference(_).toOption)

  implicit def linkEncoder(implicit base: BaseUri): Encoder.AsObject[SparqlLink] = Encoder.AsObject.instance {
    case SparqlExternalLink(id, paths, types) =>
      JsonObject("@id" -> id.asJson, "@type" -> types.asJson, "paths" -> paths.asJson)
    case SparqlResourceLink(resource)         =>
      implicit val pathsEncoder: Encoder.AsObject[List[Iri]] =
        Encoder.AsObject.instance(paths => JsonObject("paths" -> Json.fromValues(paths.map(_.asJson))))
      resource.asJsonObject
  }
}
