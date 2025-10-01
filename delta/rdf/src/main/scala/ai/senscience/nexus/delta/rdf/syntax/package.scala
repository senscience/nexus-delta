package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.kernel.syntax.{ClassTagSyntax, IOSyntax}

package object syntax
    extends JsonSyntax
    with IriSyntax
    with JsonLdEncoderSyntax
    with IterableSyntax
    with UriSyntax
    with PathSyntax
    with ClassTagSyntax
    with IOSyntax
