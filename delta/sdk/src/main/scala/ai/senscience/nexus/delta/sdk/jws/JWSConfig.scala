package ai.senscience.nexus.delta.sdk.jws

import com.nimbusds.jose.jwk.RSAKey
import pureconfig.generic.auto.*
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigConvert, ConfigReader}

import java.security.interfaces.RSAPrivateCrtKey
import scala.concurrent.duration.FiniteDuration

sealed trait JWSConfig

object JWSConfig {
  final case object Disabled extends JWSConfig

  final case class Enabled(privateKey: RSAPrivateCrtKey, ttl: FiniteDuration) extends JWSConfig {
    val rsaKey: RSAKey = RSAUtils.generateRSAKeyFromPrivate(privateKey)
  }

  implicit private val privateKeyConvert: ConfigConvert[RSAPrivateCrtKey] =
    ConfigConvert.viaStringTry[RSAPrivateCrtKey](RSAUtils.parseRSAPrivateKey, _.toString)

  implicit val delegationReader: ConfigReader[JWSConfig] = deriveReader
}
