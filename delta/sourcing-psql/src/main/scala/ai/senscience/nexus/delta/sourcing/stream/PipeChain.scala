package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import ai.senscience.nexus.delta.sourcing.stream.pipes.{DiscardMetadata, FilterBySchema, FilterByType, FilterDeprecated}
import cats.data.NonEmptyChain
import cats.implicits.*

/**
  * An identified collection of pipe references along with their configuration. It can be compiled into a single
  * [[Operation]] that sets the `id` in the context of all elements processed by chaining all constituents together in
  * the provided order.
  *
  * @param pipes
  *   the collection of pipe references and their configuration
  */
final case class PipeChain(
    pipes: NonEmptyChain[(PipeRef, ExpandedJsonLd)]
)

object PipeChain {

  def apply(first: (PipeRef, ExpandedJsonLd), others: (PipeRef, ExpandedJsonLd)*): PipeChain =
    new PipeChain(NonEmptyChain(first, others*))

  /**
    * Create a [[PipeChain]] from the given constraints
    * @param resourceSchemas
    *   filter on schemas if non empty
    * @param resourceTypes
    *   filter on resource types if non empty
    * @param includeMetadata
    *   include resource metadata if true
    * @param includeDeprecated
    *   include deprecated resources if true
    */
  def apply(
      resourceSchemas: IriFilter,
      resourceTypes: IriFilter,
      includeMetadata: Boolean,
      includeDeprecated: Boolean
  ): Option[PipeChain] = {
    val resourceSchemasPipeChain = resourceSchemas.asRestrictedTo.map(FilterBySchema(_)).toList
    val resourceTypesPipeChain   = resourceTypes.asRestrictedTo.map(FilterByType(_)).toList

    NonEmptyChain
      .fromSeq {
        resourceSchemasPipeChain ++ resourceTypesPipeChain ++
          List(
            !includeDeprecated -> FilterDeprecated(),
            !includeMetadata   -> DiscardMetadata()
          ).mapFilter { case (b, p) => Option.when(b)(p) }
      }
      .map(PipeChain(_))
  }

}
