package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import cats.effect.IO

/**
  * A [[ContextValue]] that is specialized for metadata
  */
final case class MetadataContextValue(value: ContextValue) extends AnyVal {

  /**
    * Combines the current [[MetadataContextValue]] context with a passed [[MetadataContextValue]] context. If a keys
    * are is repeated in both contexts, the one in ''that'' will override the current one.
    *
    * @param that
    *   another metadata context to be merged with the current
    */
  def merge(that: MetadataContextValue): MetadataContextValue = MetadataContextValue(value.merge(that.value))
}

object MetadataContextValue {

  /**
    * An empty [[MetadataContextValue]]
    */
  val empty: MetadataContextValue = MetadataContextValue(ContextValue.empty)

  /**
    * Loads a [[MetadataContextValue]] form the passed ''resourcePath''
    */
  final def fromFile(resourcePath: String)(using ClasspathResourceLoader): IO[MetadataContextValue] =
    ContextValue.fromFile(resourcePath).map(MetadataContextValue.apply)
}
