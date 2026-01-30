package ai.senscience.nexus.delta.sdk.views

import doobie.{Get, Put}
import io.circe.{Decoder, Encoder}

/**
  * Indexing revision for a view
  *
  * The indexing revision refers to the last revision where was introduced a change impacting indexing
  *
  * Ex:
  *   - Updating the name of a view should not update the indexing rev
  *   - Changing the types of resources to index will
  */
final case class IndexingRev(value: Int) extends AnyVal

object IndexingRev {

  val init = IndexingRev(1)

  given Encoder[IndexingRev] = Encoder.encodeInt.contramap(_.value)
  given Decoder[IndexingRev] = Decoder.decodeInt.map { IndexingRev(_) }

  given Get[IndexingRev] = Get[Int].map(IndexingRev(_))
  given Put[IndexingRev] = Put[Int].contramap(_.value)
}
