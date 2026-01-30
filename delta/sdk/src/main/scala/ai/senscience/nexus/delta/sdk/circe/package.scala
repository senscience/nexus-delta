package ai.senscience.nexus.delta.sdk

import cats.Order
import cats.data.NonEmptyMap
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, JsonObject}

package object circe {

  object nonEmptyMap {

    given dropKeyEncoder: [K, V] => (encodeV: Encoder[V]) => Encoder[NonEmptyMap[K, V]] =
      Encoder.instance { map =>
        map.toNel.map(_._2).asJson
      }

    def dropKeyDecoder[K, V](extract: V => K)(using orderK: Order[K], decodeV: Decoder[V]): Decoder[NonEmptyMap[K, V]] =
      Decoder.decodeNonEmptyList[V].map {
        _.map { v => extract(v) -> v }.toNem
      }

  }

  extension (obj: JsonObject) {
    def dropNulls: JsonObject = dropNullValues(obj)
  }

  def dropNullValues(obj: JsonObject): JsonObject = obj.filter { case (_, v) => !v.isNull }
}
