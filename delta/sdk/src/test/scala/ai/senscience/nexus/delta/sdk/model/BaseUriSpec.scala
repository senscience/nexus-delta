package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.BaseSpec
import pureconfig.ConfigSource

class BaseUriSpec extends BaseSpec {

  "A BaseUri config reader" should {
    "correctly slice the last path segment" in {
      val mapping = Map(
        "http://localhost"                        -> BaseUri.withoutPrefix(uri"http://localhost"),
        "http://localhost:8080"                   -> BaseUri.withoutPrefix(uri"http://localhost:8080"),
        "http://localhost:8080/"                  -> BaseUri.withoutPrefix(uri"http://localhost:8080"),
        "http://localhost:8080//"                 -> BaseUri.withoutPrefix(uri"http://localhost:8080"),
        "http://localhost:8080/a//b/v1//"         -> BaseUri(uri"http://localhost:8080/a/b", Label.unsafe("v1")),
        "http://localhost:8080/a//b/v1//?c=d#e=f" -> BaseUri(uri"http://localhost:8080/a/b", Label.unsafe("v1"))
      )
      forAll(mapping) { case (input, expected) =>
        source(input).load[BaseUri].rightValue shouldEqual expected
      }
    }

    "fail config loading" in {
      val list = List("http://localhost/,", "http://localhost/%20", "localhost/a/b")
      forAll(list) { input =>
        source(input).load[BaseUri].leftValue
      }
    }
  }

  private def source(input: String): ConfigSource =
    ConfigSource
      .string(s"""base-uri = "$input"""")
      .at("base-uri")

}
