package ai.senscience.nexus.delta.rdf.utils

import io.circe.*
import io.circe.syntax.*

trait JsonUtils {

  /**
    * Checks whether or not the passed ''json'' is empty
    */
  def isEmpty(json: Json): Boolean =
    json.asObject.exists(_.isEmpty) || json.asArray.exists(_.isEmpty) || json.asString.exists(_.isEmpty)

  /**
    * Map value of all instances of a key.
    * @param json
    *   the json to apply to
    * @param key
    *   the key
    * @param f
    *   the function to apply
    * @return
    *   [[Json]] with all values of a key mapped
    */
  def mapAllKeys(json: Json, key: String, f: Json => Json): Json = {
    def inner(obj: JsonObject): JsonObject = obj(key) match {
      case Some(value) =>
        obj.add(key, f(value)).mapValues(mapAllKeys(_, key, f))
      case None        => obj.mapValues(mapAllKeys(_, key, f))
    }
    json.arrayOrObject(
      json,
      arr => Json.fromValues(arr.map(j => mapAllKeys(j, key, f))),
      obj => Json.fromJsonObject(inner(obj))
    )
  }

  /**
    * Removes the provided keys from the top object on the json.
    */
  def removeKeys(json: Json, keys: String*): Json = {
    def inner(obj: JsonObject): JsonObject = obj.filterKeys(!keys.contains(_))
    json.arrayOrObject(
      json,
      arr => Json.fromValues(arr.map(j => removeKeys(j, keys*))),
      obj => Json.fromJsonObject(inner(obj))
    )
  }

  /**
    * Remove metadata keys (starting with `_`) from the json
    */
  def removeMetadataKeys(json: Json): Json = {
    json.arrayOrObject(
      json,
      arr => Json.fromValues(arr),
      obj => Json.fromJsonObject(obj.filterKeys(!_.startsWith("_")))
    )
  }

  /**
    * Extract all the values found from the passed ''keys''
    *
    * @param json
    *   the target json
    * @param keys
    *   the keys from where to extract the Json values
    */
  def extractValuesFrom(json: Json, keys: String*): Set[Json] = {

    def inner(obj: JsonObject): Iterable[Json] =
      obj.toVector.flatMap {
        case (k, v) if keys.contains(k) => Vector(v)
        case (_, v)                     => extractValuesFrom(v, keys*)
      }

    json
      .arrayOrObject(
        Vector.empty[Json],
        arr => arr.flatMap(j => extractValuesFrom(j, keys*)),
        obj => inner(obj).toVector
      )
      .toSet
  }

  /**
    * Removes the provided values from everywhere on the json.
    */
  def removeAllValues[A: Encoder](json: Json, values: A*): Json =
    removeNested(json, values.map(v => (_ => true, vv => vv == v.asJson)))

  /**
    * Removes the provided keys from everywhere on the json.
    */
  def removeAllKeys(json: Json, keys: String*): Json =
    removeNested(json, keys.map(k => (kk => kk == k, _ => true)))

  /**
    * Replace in the passed ''json'' the found key ''fromKey'' and value ''fromValue'' with the value in ''toValue''
    */
  def replace[A: Encoder, B: Encoder](json: Json, fromKey: String, fromValue: A, toValue: B): Json = {
    replace(json, (k: String, v: Json) => fromKey == k && fromValue.asJson == v, toValue)
  }

  /**
    * Replace in the passed ''json'' the found key ''fromKey'' with the value in ''toValue''
    */
  def replace[A: Encoder](json: Json, fromKey: String, toValue: A): Json = {
    replace(json, (k: String, _: Json) => fromKey == k, toValue)
  }

  /**
    * Replace in the passed ''json'' the found value ''fromValue'' with the value in ''toValue''
    */
  def replace[A: Encoder, B: Encoder](json: Json, fromValue: A, toValue: B): Json = {
    replace(json, (_: String, v: Json) => fromValue.asJson == v, toValue)
  }

  /**
    * Replace in the passed ''json'' the found key value pairs that matches ''f'' with the value in ''toValue''
    */
  def replace[A: Encoder](json: Json, f: (String, Json) => Boolean, toValue: A): Json = {
    def inner(obj: JsonObject): JsonObject =
      JsonObject.fromIterable(
        obj.toVector.map {
          case (k, v) if f(k, v) => k -> toValue.asJson
          case (k, v)            => k -> replace(v, f, toValue)
        }
      )
    json.arrayOrObject(
      json,
      arr => Json.fromValues(arr.map(j => replace(j, f, toValue))),
      obj => Json.fromJsonObject(inner(obj))
    )
  }

  private def removeNested(json: Json, keyValues: Seq[(String => Boolean, Json => Boolean)]): Json = {
    def inner(obj: JsonObject): JsonObject =
      JsonObject.fromIterable(
        obj.filter { case (k, v) => !keyValues.exists { case (fk, fv) => fk(k) && fv(v) } }.toVector.map {
          case (k, v) => k -> removeNested(v, keyValues)
        }
      )
    json.arrayOrObject(
      json,
      arr => Json.fromValues(arr.map(j => removeNested(j, keyValues))),
      obj => Json.fromJsonObject(inner(obj))
    )
  }

  /**
    * Sort all the keys in the passed ''json''.
    *
    * @param json
    *   the json to sort
    * @param ordering
    *   the sorting strategy
    */
  def sort(json: Json)(implicit ordering: JsonKeyOrdering): Json = {

    def inner(obj: JsonObject): JsonObject =
      JsonObject.fromIterable(obj.toVector.sortBy(_._1)(ordering).map { case (k, v) => k -> sort(v) })

    json.arrayOrObject[Json](json, arr => Json.fromValues(arr.map(sort)), obj => inner(obj).asJson)
  }
}

object JsonUtils extends JsonUtils
