package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder

/**
  * Type that represents a project payload for creation and update requests.
  *
  * @param description
  *   an optional description
  * @param apiMappings
  *   the API mappings
  * @param base
  *   an optional base Iri for generated resource IDs ending with ''/'' or ''#''
  * @param vocab
  *   an optional vocabulary for resources with no context ending with ''/'' or ''#''
  * @param enforceSchema
  *   a flag to ban unconstrained resources in this project
  */
final case class ProjectFields(
    description: Option[String],
    apiMappings: ApiMappings = ApiMappings.empty,
    base: Option[PrefixIri],
    vocab: Option[PrefixIri],
    enforceSchema: Boolean = false
) {

  /**
    * @return
    *   the current base or a generated one based on the ''baseUri'' and the project ref
    */
  def baseOrGenerated(project: ProjectRef)(implicit baseUri: BaseUri): PrefixIri =
    base.getOrElse(
      PrefixIri.unsafe(
        (baseUri.endpoint / "resources" / project.organization.value / project.project.value / "_").finalSlash.toIri
      )
    )

  /**
    * @return
    *   the current vocab or a generated one based on the ''baseUri'' and the project ref
    */
  def vocabOrGenerated(project: ProjectRef)(implicit baseUri: BaseUri): PrefixIri =
    vocab.getOrElse(
      PrefixIri.unsafe(
        (baseUri.endpoint / "vocabs" / project.organization.value / project.project.value).finalSlash.toIri
      )
    )

}

object ProjectFields {

  implicit final private val configuration: Configuration   = Configuration.default.withStrictDecoding.withDefaults
  implicit val projectFieldsDecoder: Decoder[ProjectFields] = deriveConfiguredDecoder[ProjectFields]

}
