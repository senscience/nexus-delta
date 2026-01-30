package ai.senscience.nexus.delta.sdk.instances

import org.http4s.BasicCredentials
import pureconfig.*

trait CredentialsInstances {

  given ConfigReader[BasicCredentials] =
    ConfigReader.forProduct2[BasicCredentials, String, String]("username", "password")(BasicCredentials(_, _))
}

object CredentialsInstances extends CredentialsInstances
