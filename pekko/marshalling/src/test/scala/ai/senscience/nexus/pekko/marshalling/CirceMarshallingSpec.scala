package ai.senscience.nexus.pekko.marshalling

import io.circe.literal.*
import io.circe.syntax.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{HttpEntity, MessageEntity}
import org.apache.pekko.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import language.adhocExtensions

import java.time.Instant

class CirceMarshallingSpec
    extends TestKit(ActorSystem("CirceMarshallingSpec"))
    with AnyWordSpecLike
    with Matchers
    with CirceMarshalling
    with ScalaFutures {
  import system.dispatcher

  private val id       = "myresource"
  private val resource = SimpleResource(id, 1, Instant.EPOCH, "Maria", 20)
  private val json     = json"""{"id": $id, "rev": 1, "createdAt": ${Instant.EPOCH.asJson}, "name": "Maria", "age": 20}"""

  "Converting SimpleResource to an HttpEntity" should {

    "succeed" in {
      Marshal(resource).to[MessageEntity].futureValue shouldEqual HttpEntity(`application/json`, json.noSpaces)
    }
  }
}
