package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.syntax.{ClassTagSyntax, Http4sResponseSyntax, IOSyntax, OtelSyntax}
import ai.senscience.nexus.delta.rdf.instances.{TripleInstances, UriInstances}
import ai.senscience.nexus.delta.rdf.syntax.{IriSyntax, IterableSyntax, JsonLdEncoderSyntax, JsonSyntax, PathSyntax, UriSyntax}
import ai.senscience.nexus.delta.sdk.instances.{CredentialsInstances, IdentityInstances, IriInstances, ProjectRefInstances}
import ai.senscience.nexus.delta.sdk.syntax.{HttpResponseFieldsSyntax, IriEncodingSyntax}

/**
  * Aggregate instances and syntax from rdf plus the current sdk instances and syntax to avoid importing multiple
  * instances and syntax
  */
package object implicits
    extends TripleInstances
    with UriInstances
    with CredentialsInstances
    with Http4sResponseSyntax
    with IdentityInstances
    with IriInstances
    with ProjectRefInstances
    with JsonSyntax
    with IriSyntax
    with IriEncodingSyntax
    with JsonLdEncoderSyntax
    with UriSyntax
    with PathSyntax
    with IterableSyntax
    with OtelSyntax
    with HttpResponseFieldsSyntax
    with IOSyntax
    with ClassTagSyntax
