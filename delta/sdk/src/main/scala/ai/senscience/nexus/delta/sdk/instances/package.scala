package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.instances.Fs2Instances
import ai.senscience.nexus.delta.kernel.syntax.Http4sResponseSyntax
import ai.senscience.nexus.delta.rdf.instances.{TripleInstances, UriInstances}

package object instances
    extends CredentialsInstances
    with Fs2Instances
    with Http4sResponseSyntax
    with HttpResponseFieldsInstances
    with IdentityInstances
    with IriInstances
    with ProjectRefInstances
    with TripleInstances
    with UriInstances
