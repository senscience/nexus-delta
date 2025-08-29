package ai.senscience.nexus.delta.sourcing.stream

package object pipes {

  val defaultPipes: Set[PipeDef] = Set(
    DiscardMetadata,
    FilterDeprecated,
    SourceAsText,
    FilterByType,
    FilterBySchema,
    DataConstructQuery,
    SelectPredicates,
    DefaultLabelPredicates
  )

}
