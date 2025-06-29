package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.implicits.CirceInstances.{jsonCodecDropNull, jsonSourceCodec}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import cats.syntax.all.*
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import doobie.{Get, Put}
import io.circe.generic.extras.Configuration
import io.circe.syntax.*
import io.circe.{Codec, Encoder, Json}

/**
  * Defines how to extract an id from an event/state and how to serialize and deserialize it
  * @param encodeId
  *   to encode the identifier as an Iri
  * @param codec
  *   the Circe codec to serialize/deserialize the event/state from the database
  */
final class Serializer[Id, Value] private (
    encodeId: Id => Iri,
    val codec: Codec.AsObject[Value],
    val jsonIterCodec: JsonValueCodec[Json]
) {

  def putId: Put[Id] = Put[Iri].contramap(encodeId)

  def getValue: Get[Value] = jsonbGet.temap(v => codec.decodeJson(v).leftMap(_.message))

  def putValue: Put[Value] = jsonbPut(jsonIterCodec).contramap(codec(_))
}

object Serializer {
  val circeConfiguration: Configuration = Configuration.default.withDiscriminator("@type")

  /**
    * Defines a serializer with the default printer serializing null values with a custom [[Id]] type
    */
  def apply[Id, Value](extractId: Id => Iri)(implicit codec: Codec.AsObject[Value]): Serializer[Id, Value] =
    new Serializer(extractId, codec, jsonSourceCodec)

  /**
    * Defines a serializer with the default printer serializing null values with an [[Iri]] id
    */
  def apply[Value]()(implicit codec: Codec.AsObject[Value]): Serializer[Iri, Value] =
    apply(identity[Iri])(codec)

  /**
    * Defines a serializer with the default printer ignoring null values with a custom [[Id]]
    */
  def dropNulls[Id, Value](extractId: Id => Iri)(implicit codec: Codec.AsObject[Value]): Serializer[Id, Value] =
    new Serializer(extractId, codec, jsonCodecDropNull)

  /**
    * Defines a serializer with the default printer ignoring null values with a custom [[Id]] and injecting the resource
    * types
    */
  def dropNullsInjectType[Id, State <: ScopedState](
      extractId: Id => Iri
  )(implicit codec: Codec.AsObject[State]): Serializer[Id, State] = {
    val codecWithType = Codec.AsObject.from(
      codec,
      Encoder.AsObject.instance[State] { state =>
        codec.mapJsonObject(_.+:("types" := state.types)).encodeObject(state)
      }
    )
    dropNulls(extractId)(codecWithType)
  }

  /**
    * Defines a serializer with the default printer ignoring null values with an [[Iri]] id
    */
  def dropNulls[Value]()(implicit codec: Codec.AsObject[Value]): Serializer[Iri, Value] =
    dropNulls(identity[Iri])(codec)

  /**
    * Defines a serializer for states with the default printer ignoring null values with an [[Iri]] id and injecting the
    * resource types
    */
  def dropNullsInjectType[State <: ScopedState]()(implicit codec: Codec.AsObject[State]): Serializer[Iri, State] =
    dropNullsInjectType(identity[Iri])(codec)

}
