package ai.senscience.nexus.delta.sdk.instances

import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sourcing.ProgressStatistics
import ai.senscience.nexus.delta.sourcing.model.Tags
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.RemainingElems

trait HttpResponseFieldsInstances {
  given offsetResponseFields: HttpResponseFields[Offset] = HttpResponseFields.defaultOk

  given progressStatisticsResponseFields: HttpResponseFields[ProgressStatistics] = HttpResponseFields.defaultOk

  given remainingElemsHttpResponseFields: HttpResponseFields[RemainingElems] = HttpResponseFields.defaultOk

  given tagsHttpResponseFields: HttpResponseFields[Tags] = HttpResponseFields.defaultOk
}
