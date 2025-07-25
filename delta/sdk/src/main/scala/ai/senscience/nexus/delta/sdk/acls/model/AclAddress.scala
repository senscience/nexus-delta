package ai.senscience.nexus.delta.sdk.acls.model

import ai.senscience.nexus.delta.kernel.error.FormatError
import ai.senscience.nexus.delta.sdk.error.FormatErrors.IllegalAclAddressFormatError
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import cats.data.NonEmptyList
import cats.syntax.all.*
import doobie.{Get, Put}
import io.circe.{Decoder, Encoder, KeyDecoder}

/**
  * Enumeration of possible ACL addresses. An ACL address is the address where a certain ACL is anchored.
  */
sealed trait AclAddress extends Product with Serializable {

  /**
    * the string representation of the address
    */
  def string: String

  /**
    * @return
    *   the parent [[AclAddress]] for the current address, or None when there is no parent
    */
  def parent: Option[AclAddress]

  /**
    * @return
    *   an ordered list of ancestors (that includes this address) first to last being project to root
    */
  def ancestors: NonEmptyList[AclAddress]

  override def toString: String = string
}

object AclAddress {

  final private[sdk] val orgAddressRegex  = s"^/(${Label.regex.regex})$$".r
  final private[sdk] val projAddressRegex = s"^/(${Label.regex.regex})/(${Label.regex.regex})$$".r

  implicit final def fromProject(project: ProjectRef): AclAddress = Project(project)
  implicit final def fromOrg(label: Label): AclAddress            = Organization(label)

  /**
    * Attempts to construct an AclAddress from the provided string. The accepted formats are the ones generated from the
    * [[AclAddress.string]] functions. The validation make use of the [[Label.regex]] to ensure compatibility with a
    * valid [[Label]].
    *
    * @param string
    *   the string representation of the AclAddress
    */
  final def fromString(string: String): Either[FormatError, AclAddress] = string match {
    case Root.string                 => Right(Root)
    case orgAddressRegex(org)        => Right(Organization(Label.unsafe(org))) // safe because the Label is already validated
    case projAddressRegex(org, proj) => Right(Project(Label.unsafe(org), Label.unsafe(proj)))
    case _                           => Left(IllegalAclAddressFormatError())
  }

  final def fromScope(scope: Scope): AclAddress = scope match {
    case Scope.Root             => Root
    case Scope.Org(label)       => Organization(label)
    case Scope.Project(project) => Project(project)
  }

  type Root = Root.type

  /**
    * The top level address.
    */
  final case object Root extends AclAddress {

    val string: String                      = "/"
    val parent: Option[AclAddress]          = None
    val ancestors: NonEmptyList[AclAddress] = NonEmptyList.one(this)
  }

  /**
    * The organization level address.
    */
  final case class Organization(org: Label) extends AclAddress {

    val string                              = s"/$org"
    val parent: Option[AclAddress]          = Some(Root)
    val ancestors: NonEmptyList[AclAddress] = NonEmptyList.of(this, Root)
  }

  /**
    * The project level address.
    */
  final case class Project(org: Label, project: Label) extends AclAddress {

    val string                              = s"/$org/$project"
    val parent: Option[AclAddress]          = Some(Organization(org))
    val ancestors: NonEmptyList[AclAddress] = NonEmptyList.of(this, Organization(org), Root)
  }

  object Project {

    /**
      * Create project level address from [[ProjectRef]].
      */
    def apply(projectRef: ProjectRef): Project = Project(projectRef.organization, projectRef.project)
  }

  implicit val aclAddressGet: Get[AclAddress] = Get[String].temap(AclAddress.fromString(_).leftMap(_.getMessage))
  implicit val aclAddressPut: Put[AclAddress] = Put[String].contramap(_.string)

  implicit val aclAddressOrdering: Ordering[AclAddress] = Ordering.by(_.string)

  implicit val aclAddressKeyDecoder: KeyDecoder[AclAddress] = KeyDecoder.instance(AclAddress.fromString(_).toOption)

  implicit val aclAddressEncoder: Encoder[AclAddress] = Encoder.encodeString.contramap(_.string)
  implicit val aclAddressDecoder: Decoder[AclAddress] = Decoder.decodeString.emap { str =>
    AclAddress.fromString(str).leftMap(_.getMessage)
  }
}
