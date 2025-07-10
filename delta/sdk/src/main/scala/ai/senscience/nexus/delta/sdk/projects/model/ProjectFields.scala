package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig.PrefixConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.Decoder

/**
  * Type that represents a project payload for creation and update requests.
  *
  * @param description
  *   an optional description
  * @param apiMappings
  *   the API mappings
  * @param base
  *   a base Iri for generated resource IDs ending with ''/'' or ''#'' defaulting to the template defined in the project
  *   config
  * @param vocab
  *   a vocabulary for resources with no context ending with ''/'' or ''#'' defaulting to the template defined in the
  *   project config
  * @param enforceSchema
  *   a flag to ban unconstrained resources in this project
  */
final case class ProjectFields(
    description: Option[String],
    apiMappings: ApiMappings,
    base: PrefixIri,
    vocab: PrefixIri,
    enforceSchema: Boolean
)

object ProjectFields {

  def decoder(project: ProjectRef, prefixConfig: PrefixConfig): Decoder[ProjectFields] = Decoder.instance { hc =>
    for {
      description   <- hc.get[Option[String]]("description")
      apiMappings   <- hc.getOrElse[ApiMappings]("apiMappings")(ApiMappings.empty)
      base          <- hc.getOrElse[PrefixIri]("base")(prefixConfig.base.create(project))
      vocab         <- hc.getOrElse[PrefixIri]("vocab")(prefixConfig.vocab.create(project))
      enforceSchema <- hc.getOrElse[Boolean]("enforceSchema")(false)
    } yield ProjectFields(description, apiMappings, base, vocab, enforceSchema)

  }

}
