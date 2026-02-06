package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri.unsafe
import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import cats.Order
import cats.syntax.all.*
import io.circe.*
import org.apache.jena.rfc3986.IRI3986
import org.http4s.{Query, Uri}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import java.util.UUID

/**
  * Represents an [[Iri]] or a [[BNode]]
  */
sealed trait IriOrBNode extends Product with Serializable {

  /**
    * @return
    *   true if the current value is an [[Iri]], false otherwise
    */
  def isIri: Boolean

  /**
    * @return
    *   true if the current value is an [[BNode]], false otherwise
    */
  def isBNode: Boolean

  /**
    * @return
    *   Some(iri) if the current value is an [[Iri]], None otherwise
    */
  def asIri: Option[Iri]

  /**
    * @return
    *   Some(bnode) if the current value is a [[BNode]], None otherwise
    */
  def asBNode: Option[BNode]

  /**
    * The rdf string representation of the [[Iri]] or [[BNode]]
    */
  def rdfFormat: String
}

object IriOrBNode {

  /**
    * A simple [[Iri]] representation backed up by Jena [[IRI]].
    *
    * @param value
    *   the underlying Jena [[IRI]]
    */
  final case class Iri private (val value: IRI3986) extends IriOrBNode {

    /**
      * Extract the query parameters as key and values
      */
    def query(): Query =
      Query.unsafeFromString(rawQuery())

    /**
      * Extract the query parameters as String
      */
    def rawQuery(): String = Option(value.query()).getOrElse("")

    /**
      * Removes each encounter of the passed query parameter keys from the current Iri query parameters
      *
      * @param keys
      *   the keys to remove
      */
    def removeQueryParams(keys: String*): Iri =
      if rawQuery().isEmpty then this
      else {
        queryParams {
          keys.foldLeft(query()) { case (acc, key) =>
            acc.removeQueryParam(key)
          }
        }
      }

    /**
      * Override the current query parameters with the passed ones
      */
    def queryParams(query: Query): Iri =
      if Option(value.authority()).nonEmpty then {
        Iri.unsafe(
          scheme = Option(value.scheme()),
          userInfo = Option(value.userInfo()),
          host = Option(value.host()),
          port = Option(value.port()),
          path = Option(value.path()),
          query = Option.when(query.nonEmpty)(query.toString()),
          fragment = Option(value.fragment())
        )
      } else {
        Iri.unsafe(
          scheme = Option(value.scheme()),
          path = path,
          query = Option.when(query.nonEmpty)(query.toString()),
          fragment = fragment
        )
      }

    /**
      * Is valid according to the IRI rfc
      */
    def isValid: Boolean =
      value.hasViolations

    /**
      * Defines if the iri is suitable for a usage in RDF.
      *
      * @return
      *   true if this IRI has a scheme specified, false otherwise
      */
    def isReference: Boolean =
      value.isRootless || value.scheme() != null

    /**
      * Is this Iri a relative reference without a scheme specified.
      *
      * @return
      *   true if the Iri is a relative reference, false otherwise
      */
    def isRelative: Boolean =
      value.isRelative

    /**
      * @return
      *   true if the current ''iri'' starts with the passed ''other'' iri, false otherwise
      */
    def startsWith(other: Iri): Boolean =
      toString.startsWith(other.toString)

    /**
      * @return
      *   the resulting string from stripping the passed ''iri'' to the current iri.
      */
    def stripPrefix(iri: Iri): String =
      stripPrefix(iri.toString)

    /**
      * @return
      *   the resulting string from stripping the passed ''prefix'' to the current iri.
      */
    def stripPrefix(prefix: String): String =
      toString.stripPrefix(prefix)

    /**
      * An Iri is a prefix mapping if it ends with `/` or `#`
      */
    def isPrefixMapping: Boolean =
      toString.endsWith("/") || toString.endsWith("#")

    /**
      * Adds a segment to the end of the Iri
      */
    def /(segment: String): Iri = {
      lazy val segmentStartsWithSlash = segment.startsWith("/")
      lazy val iriEndsWithSlash       = toString.endsWith("/")
      if iriEndsWithSlash && segmentStartsWithSlash then unsafe(s"$value${segment.drop(1)}")
      else if iriEndsWithSlash || segmentStartsWithSlash then unsafe(s"$value$segment")
      else unsafe(s"$value/$segment")
    }

    /**
      * Constructs a [[Uri]] from the current [[Iri]]
      */
    def toUri: Either[String, Uri] = Uri.fromString(toString).leftMap(_.sanitized)

    /**
      * @return
      *   the IRI scheme
      */
    def scheme: Option[String] = Option(value.scheme())

    override lazy val toString: String = value.toString

    override val rdfFormat: String = s"<$toString>"

    override val isIri: Boolean = true

    override val isBNode: Boolean = false

    override val asIri: Option[Iri] = Some(this)

    override val asBNode: Option[BNode] = None

    /**
      * Returns a new absolute Iri resolving the current relative [[Iri]] with the passed absolute [[Iri]]. If the
      * current [[Iri]] is absolute, there is nothing to resolve against and the current [[Iri]] is returned. If the
      * passed [[Iri]] is not absolute, there is nothing to resolve against and the current [[Iri]] is returned.
      */
    def resolvedAgainst(iri: Iri): Iri =
      if isReference then this
      else if iri.isReference then {
        val relative = if toString.endsWith("/") then toString.takeRight(1) else toString
        val absolute = if iri.toString.startsWith("/") then iri.toString.take(1) else iri.toString
        Iri.unsafe(s"$absolute/$relative")

      } else this

    /**
      * @return
      *   the Iri path, if present
      */
    def path: Option[String] = Option(value.path())

    /**
      * @return
      *   the last Iri path segment, if present
      */
    def lastSegment: Option[String] =
      value.pathSegments().lastOption

    /**
      * @return
      *   the Iri fragment, if present
      */
    def fragment: Option[String] =
      Option(value.fragment())
  }

  object Iri {

    /**
      * Construct an [[Iri]] safely.
      *
      * @param string
      *   the string from which to construct an [[Iri]]
      */
    def apply(string: String): Either[String, Iri] = {
      val iri = unsafe(string)
      Option.when(!iri.isValid)(iri).toRight(s"'$string' is not an IRI")
    }

    /**
      * Construct an [[Iri]] from its raw components.
      *
      * @param scheme
      *   the optional scheme segment
      * @param userInfo
      *   the optional user info segment
      * @param host
      *   the optional host segment
      * @param port
      *   the optional port
      * @param path
      *   the optional path segment
      * @param query
      *   the optional query segment
      * @param fragment
      *   the optional fragment segment
      */
    def unsafe(
        scheme: Option[String],
        userInfo: Option[String],
        host: Option[String],
        port: Option[String],
        path: Option[String],
        query: Option[String],
        fragment: Option[String]
    ): Iri = {
      val sb = new StringBuilder
      scheme.foreach(sb.append(_).append(':'))
      sb.append("//")
      userInfo.foreach(sb.append(_).append('@'))
      host.foreach(sb.append)
      port.foreach(sb.append(':').append(_))
      path.foreach(sb.append)
      query.foreach(sb.append("?").append(_))
      fragment.foreach(sb.append('#').append(_))
      Iri.unsafe(sb.toString())
    }

    /**
      * Construct an [[Iri]] from its raw components
      *
      * @param scheme
      *   the optional scheme segment
      * @param path
      *   the optional path segment
      * @param query
      *   the optional query segment
      * @param fragment
      *   the optional fragment segment
      */
    def unsafe(
        scheme: Option[String],
        path: Option[String],
        query: Option[String],
        fragment: Option[String]
    ): Iri = {
      val sb = new StringBuilder
      scheme.foreach(sb.append(_).append(':'))
      path.foreach(sb.append)
      query.foreach(sb.append("?").append(_))
      fragment.foreach(sb.append('#').append(_))
      Iri.unsafe(sb.toString())
    }

    /**
      * Construct a reference [[Iri]] safely.
      *
      * @param string
      *   the string from which to construct an [[Iri]]
      */
    def reference(string: String): Either[String, Iri] =
      apply(string).flatMap(iri => Option.when(iri.isReference)(iri).toRight(s"'$string' is not an absolute IRI"))

    /**
      * Construct an IRI without checking the validity of the format.
      */
    def unsafe(string: String): Iri =
      new Iri(IRI3986.createAny(string))

    given iriDecoder: Decoder[Iri] = Decoder.decodeString.emap(apply)
    given iriEncoder: Encoder[Iri] = Encoder.encodeString.contramap(_.toString)
    given iriCodec: Codec[Iri]     = Codec.from(iriDecoder, iriEncoder)

    given iriKeyEncoder: KeyEncoder[Iri] = KeyEncoder.encodeKeyString.contramap(_.toString)
    given iriKeyDecoder: KeyDecoder[Iri] = KeyDecoder.instance(reference(_).toOption)

    given iriOrdering: Ordering[Iri] = Ordering.by(_.toString)
    given iriOrder: Order[Iri]       = Order.fromOrdering

    given ConfigReader[Iri] =
      ConfigReader.fromString(str => Iri(str).leftMap(err => CannotConvert(str, classOf[Iri].getSimpleName, err)))
  }

  /**
    * A [[BNode]] representation holding its label value
    */
  final case class BNode private (value: String) extends IriOrBNode {

    override def toString: String = value

    override val rdfFormat: String = s"_:B$toString"

    override val isIri: Boolean = false

    override val isBNode: Boolean = true

    override val asIri: Option[Iri] = None

    override val asBNode: Option[BNode] = Some(this)
  }

  object BNode {

    /**
      * Creates a random blank node
      */
    def random: BNode = BNode(UUID.randomUUID().toString.replaceAll("-", ""))

    /**
      * Unsafely creates a [[BNode]]
      *
      * @param anonId
      *   the string value of the bnode
      */
    def unsafe(anonId: String): BNode =
      BNode(anonId)

    given bNodeDecoder: Decoder[BNode] = Decoder.decodeString.map(BNode.apply)
    given bNodeEncoder: Encoder[BNode] = Encoder.encodeString.contramap(_.toString)
  }

  given iriOrBNodeDecoder: Decoder[IriOrBNode] =
    Decoder.decodeString.emap(Iri.reference).or(Decoder.decodeString.map(BNode.unsafe))

  given iriOrBNodeEncoder: Encoder[IriOrBNode] = Encoder.encodeString.contramap(_.toString)

}
