package ai.senscience.nexus.delta.rdf.utils

import ai.senscience.nexus.delta.rdf.Fixtures
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.Json
import io.circe.syntax.*

class JsonUtilsSuite extends NexusSuite with Fixtures {

  test("Json is empty") {
    List(Json.obj(), Json.arr(), Json.fromString("")).foreach { json =>
      assert(json.isEmpty(), s"$json should be considered as empty")
    }
  }

  test("Json is not empty") {
    List(json"""{"k": "v"}""", Json.arr(json"""{"k": "v"}"""), "abc".asJson, 2.asJson).foreach { json =>
      assert(!json.isEmpty(), s"$json should be considered as empty")
    }
  }

  test("Remove top keys on a Json object") {
    val json = json"""{"key": "value", "@context": {"@vocab": "${vocab.value}"}, "key2": {"key": "value"}}"""
    assertEquals(json.removeKeys("key", "@context", "key"), json"""{"key2": {"key": "value"}}""")
    assertEquals(json.removeKeys("key", "@context", "key2"), Json.obj())
  }

  test("Remove top keys on a Json array") {
    val json = json"""[{"key": "value", "key2": {"key": "value"}}, { "@context": {"@vocab": "${vocab.value}"} }]"""
    assertEquals(json.removeKeys("key", "@context"), json"""[{"key2": {"key": "value"}}, {}]""")
  }

  test("Remove keys on a Json object") {
    val json = json"""{"key": "value", "@context": {"@vocab": "${vocab.value}"}, "key2": {"key": "value"}}"""
    assertEquals(json.removeAllKeys("key", "@context"), json"""{"key2": {}}""")
    assertEquals(json.removeAllKeys("key", "@context", "key2"), Json.obj())
  }

  test("Remove all matching keys on a Json array") {
    val json = json"""[{"key": "value", "key2": {"key": "value"}}, { "@context": {"@vocab": "${vocab.value}"} }]"""
    assertEquals(json.removeAllKeys("key", "@context"), json"""[{"key2": {}}, {}]""")
  }

  test("Remove all matching values on a Json array") {
    val json = json"""[{"key": "value", "key2": {"key": "value"}}, { "@context": {"@vocab": "${vocab.value}"} }]"""
    assertEquals(json.removeAllValues("value", vocab.value.toString), json"""[{"key2": {}}, { "@context": {} }]""")
  }

  test("Replace matching key value") {
    val json =
      json"""[{"key": 1, "key2": {"key": "value"}}, { "key": 1, "@context": {"@vocab": "${vocab.value}"} }]"""

    val expected =
      json"""[{"key": "other", "key2": {"key": "value"}}, { "key": "other", "@context": {"@vocab": "${vocab.value}"} }]"""

    assertEquals(json.replace("key" -> 1, "other"), expected)
  }

  test("Replace matching key") {
    val json =
      json"""[{"key": 1, "key2": {"key": "value"}}, { "key": 1, "@context": {"@vocab": "${vocab.value}"} }]"""

    assertEquals(
      json.replaceKeyWithValue("key", "other"),
      json"""[{"key": "other", "key2": {"key": "other"}}, { "key": "other", "@context": {"@vocab": "${vocab.value}"} }]"""
    )
    assertEquals(json.replaceKeyWithValue("key4", "other"), json)
  }

  test("Extract the passed keys from the Json array") {
    val json =
      json"""[{"key": "value", "key2": {"key": {"key21": "value"}}}, { "@context": {"@vocab": "${vocab.value}", "key3": {"key": "value2"}} }]"""
    assertEquals(json.extractValuesFrom("key"), Set("value".asJson, json"""{"key21": "value"}""", "value2".asJson))
  }

  test("Sort its keys") {
    given JsonKeyOrdering = JsonKeyOrdering(topKeys = Seq("@id", "@type"), bottomKeys = Seq("_rev", "_project"))

    val input = json"""{
                        "name": "Maria",
                        "_rev": 5,
                        "age": 30,
                        "@id": "mariaId",
                        "friends": [
                          { "_rev": 1, "name": "Pablo", "_project": "a", "age": 20 },
                          { "name": "Laura", "_project": "b", "age": 23, "_rev": 2, "@id": "lauraId" }
                        ],
                        "@type": "Person"
                      }"""

    val expected = json"""{
                            "@id": "mariaId",
                            "@type": "Person",
                            "age": 30,
                            "friends": [
                              { "age": 20, "name": "Pablo", "_rev": 1, "_project": "a" },
                              { "@id": "lauraId", "age": 23, "name": "Laura", "_rev": 2, "_project": "b" }
                            ],
                            "name": "Maria",
                            "_rev": 5
                          }"""
    assertEquals(input.sort, expected)
  }

  test("Map value of all instances of a key") {
    val json     = json"""{
                      "key1": "somevalue",
                      "key2": "anothervalue",
                      "key3": { "key2": { "key2": "something" } }
                      }"""
    val expected = json"""{
                      "key1": "somevalue",
                      "key2": "mapped",
                      "key3": { "key2": "mapped" }
                  }"""
    assertEquals(json.mapAllKeys("key2", _ => "mapped".asJson), expected)
  }

  test("Add key and value only if NonEmpty") {
    val jobj     = jobj"""{"k": "v"}"""
    val expected = jobj"""{"k": "v", "k2": [1,2]}"""

    assertEquals(jobj.addIfNonEmpty("k2", List(1, 2)), expected)
    assertEquals(jobj.asJson.addIfNonEmpty("k2", List(1, 2)), expected.asJson)

    assertEquals(jobj.addIfNonEmpty("k2", List.empty[String]), jobj)
    assertEquals(jobj.asJson.addIfNonEmpty("k2", List.empty[String]), jobj.asJson)
  }

  test("Add key and value only if exists") {
    val jobj     = jobj"""{"k": "v"}"""
    val expected = jobj"""{"k": "v", "k2": "v2"}"""

    assertEquals(jobj.addIfExists("k2", Some("v2")), expected)
    assertEquals(jobj.asJson.addIfExists("k2", Some("v2")), expected.asJson)

    assertEquals(jobj.addIfExists[String]("k2", None), jobj)
    assertEquals(jobj.asJson.addIfExists[String]("k2", None), jobj.asJson)
  }

  test("Remove metadata keys") {
    val json     = json"""{ "k1": "v1",  "k2": { "_nested": "v2" }, "_m1": "v3", "_m2": "v4" }"""
    val expected = json"""{ "k1": "v1",  "k2": { "_nested": "v2" } }"""
    assertEquals(JsonUtils.removeMetadataKeys(json), expected)
  }

}
