package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.plugins.compositeviews.model
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef, Tags}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, Json}

import java.time.Instant
import java.util.UUID

/**
  * State for an existing composite view.
  *
  * @param id
  *   the view id
  * @param project
  *   a reference to the parent project
  * @param uuid
  *   the unique view identifier
  * @param value
  *   the view configuration
  * @param source
  *   the last original json value provided by the caller
  * @param tags
  *   the collection of tags
  * @param rev
  *   the current revision of the view
  * @param deprecated
  *   the deprecation status of the view
  * @param createdAt
  *   the instant when the view was created
  * @param createdBy
  *   the subject that created the view
  * @param updatedAt
  *   the instant when the view was last updated
  * @param updatedBy
  *   the subject that last updated the view
  */
final case class CompositeViewState(
    id: Iri,
    project: ProjectRef,
    uuid: UUID,
    value: CompositeViewValue,
    source: Json,
    tags: Tags,
    rev: Int,
    deprecated: Boolean,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends ScopedState {

  def schema: ResourceRef = model.schema

  override def types: Set[Iri] = Set(nxv.View, compositeViewType)

  lazy val asCompositeView: CompositeView = CompositeView(
    id,
    project,
    value.name,
    value.description,
    value.sources,
    value.projections,
    value.rebuildStrategy,
    uuid,
    tags,
    source,
    updatedAt
  )

  /**
    * Converts the state into a resource representation.
    */
  def toResource: ViewResource =
    ResourceF(
      id = id,
      access = ResourceAccess("views", project, id),
      rev = rev,
      types = Set(nxv.View, compositeViewType),
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      schema = schema,
      value = asCompositeView
    )
}

object CompositeViewState {

  implicit val serializer: Serializer[Iri, CompositeViewState] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration                       = Serializer.circeConfiguration
    implicit val compositeViewValueCodec: Codec[CompositeViewValue] = CompositeViewValue.databaseCodec()
    implicit val codec: Codec.AsObject[CompositeViewState]          = deriveConfiguredCodec[CompositeViewState]
    Serializer.dropNullsInjectType()
  }
}
