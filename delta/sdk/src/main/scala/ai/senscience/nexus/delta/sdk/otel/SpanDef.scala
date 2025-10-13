package ai.senscience.nexus.delta.sdk.otel

import org.typelevel.otel4s.{Attribute, Attributes}

final case class SpanDef(name: String, attributes: Attributes)

object SpanDef {
  def apply(name: String, attributes: Attribute[?]*) =
    new SpanDef(name, Attributes.fromSpecific(attributes))
}
