package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Tag}

/**
  * Contains the information that can be used for filtering when streaming states
  *
  * @param types
  *   types that a view is indexing
  * @param tag
  *   tag that a view is indexing
  */
case class SelectFilter(types: IriFilter, tag: Tag)

object SelectFilter {

  /** All types with latest tag */
  val latest: SelectFilter = SelectFilter(IriFilter.None, Tag.Latest)

  /** All types with specified tag */
  val tag: Tag => SelectFilter = SelectFilter(IriFilter.None, _)

  val tagOpt: Option[Tag] => SelectFilter = _.fold(latest)(tag(_))

}
