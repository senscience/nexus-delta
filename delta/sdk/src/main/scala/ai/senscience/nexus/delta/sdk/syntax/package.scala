package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.syntax.{ClassTagSyntax, IOSyntax, InstantSyntax, KamonSyntax}
import ai.senscience.nexus.delta.rdf.syntax.{IriSyntax, IterableSyntax, JsonLdEncoderSyntax, JsonSyntax, PathSyntax, UriSyntax}

/**
  * Aggregate syntax from rdf plus the current sdk syntax to avoid importing multiple syntax
  */
package object syntax
    extends JsonSyntax
    with IriSyntax
    with IriEncodingSyntax
    with JsonLdEncoderSyntax
    with PathSyntax
    with IterableSyntax
    with KamonSyntax
    with HttpResponseFieldsSyntax
    with ClassTagSyntax
    with IOSyntax
    with InstantSyntax
    with ProjectionErrorsSyntax
    with UriSyntax
