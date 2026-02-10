package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite
import pureconfig.ConfigSource
import org.http4s.syntax.literals.uri

class BaseUriSuite extends NexusSuite {

  test("A BaseUri config reader should correctly slice the last path segment") {
    val mapping = Map(
      "http://localhost"                        -> BaseUri.withoutPrefix(uri"http://localhost"),
      "http://localhost:8080"                   -> BaseUri.withoutPrefix(uri"http://localhost:8080"),
      "http://localhost:8080/"                  -> BaseUri.withoutPrefix(uri"http://localhost:8080"),
      "http://localhost:8080//"                 -> BaseUri.withoutPrefix(uri"http://localhost:8080"),
      "http://localhost:8080/a//b/v1//"         -> BaseUri(uri"http://localhost:8080/a/b", Label.unsafe("v1")),
      "http://localhost:8080/a//b/v1//?c=d#e=f" -> BaseUri(uri"http://localhost:8080/a/b", Label.unsafe("v1"))
    )
    mapping.foreach { case (input, expected) =>
      source(input).load[BaseUri].assertRight(expected)
    }
  }

  test("A BaseUri config reader should fail config loading") {
    val list = List("http://localhost/,", "http://localhost/%20", "localhost/a/b")
    list.foreach { input =>
      source(input).load[BaseUri].assertLeft()
    }
  }

  private def source(input: String): ConfigSource =
    ConfigSource
      .string(s"""base-uri = "$input"""")
      .at("base-uri")

}
