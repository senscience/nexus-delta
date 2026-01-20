package ai.senscience.nexus.delta.rdf.jsonld.decoder

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.schema
import ai.senscience.nexus.delta.rdf.jsonld.context.{JsonLdContext, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.{DecodingDerivationFailure, ParsingFailure}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderSuite.Menu.DrinkMenu
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderSuite.{AbsoluteIri, Drink, Menu}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderSuite.Drink.{Cocktail, Volume}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.configuration.semiauto.deriveConfigJsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.semiauto.*
import ai.senscience.nexus.delta.rdf.{Fixtures, RdfLoader}
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.time.Instant
import java.util.UUID
import scala.annotation.unused
import scala.concurrent.duration.*

class JsonLdDecoderSuite extends NexusSuite with Fixtures with RdfLoader {

  private given RemoteContextResolution = RemoteContextResolution.never

  private val jsonLd          = expandedFromJson("jsonld/decoder/cocktail.json").accepted
  private val ctx             = context("jsonld/decoder/context.json").accepted
  private val jsonLdContext   = JsonLdContext(ctx).accepted
  given Configuration         = Configuration.default.copy(context = jsonLdContext)
  given JsonLdDecoder[Volume] = deriveConfigJsonLdDecoder[Volume]

  test("Decodes a menu") {
    @unused given JsonLdDecoder[Drink] = deriveConfigJsonLdDecoder[Drink]
    given JsonLdDecoder[Menu]          = deriveConfigJsonLdDecoder[Menu]

    val expected = DrinkMenu(
      Set(
        Cocktail(
          schema + "mojito",
          alcohol = true,
          ing = Set("rum", "sugar"),
          name = "Mojito",
          steps = List("cut", "mix"),
          volume = Volume("%", 8.3),
          link = Some(schema + "mylink"),
          instant = Instant.parse("2020-11-25T21:29:38.939939Z"),
          uuid = UUID.fromString("e45e15a9-5e4c-4485-8de4-1a011d12349c"),
          hangover = 1.hour
        )
      )
    )
    jsonLd.to[Menu].assertRight(expected)
  }

  test("fail decoding a Menu") {
    @unused given JsonLdDecoder[Drink] = deriveDefaultJsonLdDecoder[Drink]
    given JsonLdDecoder[Menu]          = deriveDefaultJsonLdDecoder[Menu]
    jsonLd.to[Menu].assertLeftOf[DecodingDerivationFailure]
  }

  test("fail decoding a relative iri in @id value position") {
    val jsonLd                       = expandedFromJson("jsonld/decoder/relative-iri.json").accepted
    val ctx                          = context("jsonld/decoder/relative-iri-context.json").accepted
    val jsonLdContext                = JsonLdContext(ctx).accepted
    given Configuration              = Configuration.default.copy(context = jsonLdContext)
    given JsonLdDecoder[AbsoluteIri] = deriveConfigJsonLdDecoder[AbsoluteIri]
    jsonLd.to[AbsoluteIri].assertLeftOf[ParsingFailure]
  }
}

object JsonLdDecoderSuite {
  sealed trait Menu extends Product with Serializable

  sealed trait Drink extends Product with Serializable

  object Menu {
    final case class DrinkMenu(drinks: Set[Drink]) extends Menu

    final case class FoodMenu(name: String) extends Menu
  }

  object Drink {
    final case class Cocktail(
        id: Iri,
        alcohol: Boolean,
        ing: Set[String],
        name: String = "other",
        description: Option[String] = Some("default description"),
        link: Option[Iri] = Some(schema.Person),
        steps: List[String],
        volume: Volume,
        instant: Instant,
        uuid: UUID,
        hangover: FiniteDuration
    ) extends Drink

    final case class OtherDrink(id: Iri, name: String) extends Drink

    final case class Volume(unit: String, value: Double)
  }

  final case class AbsoluteIri(value: Iri)
}
