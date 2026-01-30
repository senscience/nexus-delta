package ai.senscience.nexus.delta.sdk.jws

import com.nimbusds.jose.jwk.RSAKey
import pureconfig.generic.semiauto.*
import pureconfig.{ConfigConvert, ConfigReader}

import java.security.interfaces.RSAPrivateCrtKey
import scala.concurrent.duration.FiniteDuration

sealed trait JWSConfig

object JWSConfig {
  case object Disabled extends JWSConfig

  final case class Enabled(privateKey: RSAPrivateCrtKey, ttl: FiniteDuration) extends JWSConfig {
    val rsaKey: RSAKey = RSAUtils.generateRSAKeyFromPrivate(privateKey)
  }

  private given ConfigConvert[RSAPrivateCrtKey] =
    ConfigConvert.viaStringTry[RSAPrivateCrtKey](RSAUtils.parseRSAPrivateKey, _.toString)

  given ConfigReader[JWSConfig] = deriveReader
}
