package ai.senscience.nexus.delta.rdf.instances

import ai.senscience.nexus.delta.kernel.http
import ai.senscience.nexus.delta.kernel.http.circe
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import io.circe.{Decoder, Encoder}
import org.http4s.Uri

trait UriInstances {

  given uriEncoder: Encoder[Uri] = circe.encodeUri
  given uriDecoder: Decoder[Uri] = http.circe.decodeUri

  given uriJsonLdEncoder: JsonLdEncoder[Uri] = JsonLdEncoder.computeFromCirce(ContextValue.empty)
  given uriJsonLdDecoder: JsonLdDecoder[Uri] =
    _.getValue(str => Uri.fromString(str).toOption.filter { u => u.path.isEmpty || u.path.absolute })

  given uriPathDecoder: Decoder[Uri.Path] =
    Decoder.decodeString.map(s => Uri.Path.unsafeFromString(s))
  given uriPathEncoder: Encoder[Uri.Path] = Encoder.encodeString.contramap(_.toString())
}

object UriInstances extends UriInstances
