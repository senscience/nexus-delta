package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.kernel.syntax.{ClassTagSyntax, IOSyntax, InstantSyntax}
import ai.senscience.nexus.delta.rdf.instances.{TripleInstances, UriInstances}
import ai.senscience.nexus.delta.rdf.syntax.{IriSyntax, IterableSyntax, JsonLdEncoderSyntax, JsonSyntax, PathSyntax, UriSyntax}

package object implicits
    extends TripleInstances
    with UriInstances
    with JsonSyntax
    with IriSyntax
    with JsonLdEncoderSyntax
    with IterableSyntax
    with UriSyntax
    with PathSyntax
    with ClassTagSyntax
    with IOSyntax
    with InstantSyntax
