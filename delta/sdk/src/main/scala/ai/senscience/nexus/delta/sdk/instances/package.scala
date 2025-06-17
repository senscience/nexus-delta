package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.instances.{TripleInstances, UriInstances}
import ch.epfl.bluebrain.nexus.delta.kernel.syntax.Http4sResponseSyntax

package object instances
    extends CredentialsInstances
    with Http4sResponseSyntax
    with HttpResponseFieldsInstances
    with IdentityInstances
    with IriInstances
    with ProjectRefInstances
    with TripleInstances
    with UriInstances
