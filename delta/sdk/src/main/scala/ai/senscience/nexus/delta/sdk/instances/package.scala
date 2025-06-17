package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.syntax.Http4sResponseSyntax
import ai.senscience.nexus.delta.rdf.instances.{TripleInstances, UriInstances}

package object instances
    extends CredentialsInstances
    with Http4sResponseSyntax
    with HttpResponseFieldsInstances
    with IdentityInstances
    with IriInstances
    with ProjectRefInstances
    with TripleInstances
    with UriInstances
