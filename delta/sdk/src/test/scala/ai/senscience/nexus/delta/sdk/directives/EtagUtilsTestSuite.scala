package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.MD5
import ai.senscience.nexus.delta.sdk.marshalling.JsonLdFormat
import ai.senscience.nexus.testkit.mu.NexusSuite
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.headers.{EntityTag, HttpEncodings}

class EtagUtilsTestSuite extends NexusSuite {

  private val value        = "test"
  private val mediaType    = MediaTypes.`application/json`
  private val jsonldFormat = JsonLdFormat.Expanded
  private val encoding     = HttpEncodings.gzip

  test("Compute the etag without a jsonld format") {
    val obtainedRaw  = EtagUtils.computeRawValue(value, mediaType, None, encoding)
    val expectedRaw  = s"${value}_${mediaType}_${encoding}"
    assertEquals(obtainedRaw, expectedRaw)
    val obtainedEtag = EtagUtils.compute(value, mediaType, None, encoding)
    val expectedEtag = EntityTag(MD5.hash(expectedRaw))
    assertEquals(obtainedEtag, expectedEtag)
  }

  test("Compute the etag with a jsonld format") {
    val obtainedRaw  = EtagUtils.computeRawValue(value, mediaType, Some(jsonldFormat), encoding)
    val expectedRaw  = s"${value}_${mediaType}_${jsonldFormat}_${encoding}"
    assertEquals(obtainedRaw, expectedRaw)
    val obtainedEtag = EtagUtils.compute(value, mediaType, Some(jsonldFormat), encoding)
    val expectedEtag = EntityTag(MD5.hash(expectedRaw))
    assertEquals(obtainedEtag, expectedEtag)
  }

}
