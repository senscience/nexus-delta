package ai.senscience.nexus.delta.plugins.blazegraph.client

import cats.effect.IO
import org.http4s.EntityDecoder

import scala.xml.Elem

trait XmlSupport {

  given xmlEntityDecoder: EntityDecoder[IO, Elem] = org.http4s.scalaxml.xmlDecoder[IO]

}
object XmlSupport extends XmlSupport {}
