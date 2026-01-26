package ai.senscience.nexus.delta.elasticsearch.client

import io.circe.Decoder

final case class UpdateByQueryResponse(task: String)

object UpdateByQueryResponse {

  given Decoder[UpdateByQueryResponse] =
    Decoder.instance { hc =>
      hc.get[String]("task").map(UpdateByQueryResponse(_))
    }

}
