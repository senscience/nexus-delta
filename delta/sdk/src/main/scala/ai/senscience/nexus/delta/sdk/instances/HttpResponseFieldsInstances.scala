package ai.senscience.nexus.delta.sdk.instances

import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sourcing.ProgressStatistics
import ai.senscience.nexus.delta.sourcing.model.Tags
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.RemainingElems

trait HttpResponseFieldsInstances {
  implicit val offsetResponseFields: HttpResponseFields[Offset] = HttpResponseFields.defaultOk

  implicit val progressStatisticsResponseFields: HttpResponseFields[ProgressStatistics] = HttpResponseFields.defaultOk

  implicit val remainingElemsHttpResponseFields: HttpResponseFields[RemainingElems] = HttpResponseFields.defaultOk

  implicit val tagsHttpResponseFields: HttpResponseFields[Tags] = HttpResponseFields.defaultOk
}
