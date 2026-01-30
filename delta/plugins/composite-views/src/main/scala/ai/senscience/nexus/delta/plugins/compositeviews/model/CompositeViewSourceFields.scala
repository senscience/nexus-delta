package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.{CrossProjectSource, ProjectSource, RemoteProjectSource}
import ai.senscience.nexus.delta.plugins.compositeviews.model.SourceType.{CrossProjectSourceType, ProjectSourceType, RemoteProjectSourceType}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.decoder.configuration.semiauto.deriveConfigJsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.{Configuration, JsonLdDecoder}
import ai.senscience.nexus.delta.sdk.instances.given
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.model.Identity.{Authenticated, Group, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Identity, IriFilter, ProjectRef}
import io.circe.Encoder
import org.http4s.Uri

import java.util.UUID

/**
  * Necessary fields to create/update a composite view source.
  */
sealed trait CompositeViewSourceFields {

  /**
    * @return
    *   the id of the source
    */
  def id: Option[Iri]

  /**
    * @return
    *   the schemas to filter by, empty means all
    */
  def resourceSchemas: IriFilter

  /**
    * @return
    *   the resource types to filter by, empty means all
    */
  def resourceTypes: IriFilter

  /**
    * @return
    *   the optional tag to filter by
    */
  def resourceTag: Option[UserTag]

  /**
    * @return
    *   whether to include deprecated resources
    */
  def includeDeprecated: Boolean

  /**
    * @return
    *   the source type
    */
  def tpe: SourceType

  /**
    * Transform from [[CompositeViewSourceFields]] to [[CompositeViewSource]]
    */
  def toSource(uuid: UUID, generatedId: Iri): CompositeViewSource
}

object CompositeViewSourceFields {

  /**
    * Necessary fields to create/update a project source.
    */
  final case class ProjectSourceFields(
      id: Option[Iri] = None,
      resourceSchemas: IriFilter = IriFilter.None,
      resourceTypes: IriFilter = IriFilter.None,
      resourceTag: Option[UserTag] = None,
      includeDeprecated: Boolean = false
  ) extends CompositeViewSourceFields {
    override def tpe: SourceType = ProjectSourceType

    override def toSource(uuid: UUID, generatedId: Iri): CompositeViewSource =
      ProjectSource(
        id.getOrElse(generatedId),
        uuid,
        resourceSchemas,
        resourceTypes,
        resourceTag,
        includeDeprecated
      )
  }

  /**
    * Necessary fields to create/update a cross project source.
    */
  final case class CrossProjectSourceFields(
      id: Option[Iri] = None,
      project: ProjectRef,
      identities: Set[Identity],
      resourceSchemas: IriFilter = IriFilter.None,
      resourceTypes: IriFilter = IriFilter.None,
      resourceTag: Option[UserTag] = None,
      includeDeprecated: Boolean = false
  ) extends CompositeViewSourceFields {
    override def tpe: SourceType = CrossProjectSourceType

    override def toSource(uuid: UUID, generatedId: Iri): CompositeViewSource = CrossProjectSource(
      id.getOrElse(generatedId),
      uuid,
      resourceSchemas,
      resourceTypes,
      resourceTag,
      includeDeprecated,
      project,
      identities
    )
  }

  /**
    * Necessary fields to create/update a remote project source.
    */
  final case class RemoteProjectSourceFields(
      id: Option[Iri] = None,
      project: ProjectRef,
      endpoint: Uri,
      resourceSchemas: IriFilter = IriFilter.None,
      resourceTypes: IriFilter = IriFilter.None,
      resourceTag: Option[UserTag] = None,
      includeDeprecated: Boolean = false
  ) extends CompositeViewSourceFields {
    override def tpe: SourceType = RemoteProjectSourceType

    override def toSource(uuid: UUID, generatedId: Iri): CompositeViewSource = RemoteProjectSource(
      id.getOrElse(generatedId),
      uuid,
      resourceSchemas,
      resourceTypes,
      resourceTag,
      includeDeprecated,
      project,
      endpoint
    )
  }

  given BaseUri => Encoder.AsObject[CompositeViewSourceFields] = {
    import io.circe.generic.extras.Configuration
    import io.circe.generic.extras.semiauto.*
    given Configuration = Configuration(
      transformMemberNames = {
        case "id"  => keywords.id
        case other => other
      },
      transformConstructorNames = {
        case "ProjectSourceFields"       => SourceType.ProjectSourceType.toString
        case "CrossProjectSourceFields"  => SourceType.CrossProjectSourceType.toString
        case "RemoteProjectSourceFields" => SourceType.RemoteProjectSourceType.toString
        case other                       => other
      },
      useDefaults = false,
      discriminator = Some(keywords.tpe),
      strictDecoding = false
    )
    deriveConfiguredEncoder[CompositeViewSourceFields]
  }

  given JsonLdDecoder[CompositeViewSourceFields] = {

    val ctx = Configuration.default.context
      .addAliasIdType("ProjectSourceFields", SourceType.ProjectSourceType.tpe)
      .addAliasIdType("CrossProjectSourceFields", SourceType.CrossProjectSourceType.tpe)
      .addAliasIdType("RemoteProjectSourceFields", SourceType.RemoteProjectSourceType.tpe)

    given Configuration           = Configuration.default.copy(context = ctx)
    given JsonLdDecoder[Identity] = deriveConfigJsonLdDecoder[Identity]
      .or(deriveConfigJsonLdDecoder[User].asInstanceOf[JsonLdDecoder[Identity]])
      .or(deriveConfigJsonLdDecoder[Group].asInstanceOf[JsonLdDecoder[Identity]])
      .or(deriveConfigJsonLdDecoder[Authenticated].asInstanceOf[JsonLdDecoder[Identity]])

    deriveConfigJsonLdDecoder[CompositeViewSourceFields]
  }
}
