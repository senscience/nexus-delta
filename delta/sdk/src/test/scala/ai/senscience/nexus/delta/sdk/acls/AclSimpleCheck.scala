package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.sdk.acls.model.{Acl, AclAddress}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Identity
import cats.effect.unsafe.implicits.*
import cats.effect.{IO, Ref}
import cats.syntax.all.*

import scala.collection.immutable

/**
  * In-memory implementation of an [[AclCheck]]
  */
abstract class AclSimpleCheck private (cache: Ref[IO, Map[AclAddress, Acl]]) extends AclCheck {

  def append(acl: Acl): IO[Unit] =
    cache.updateAndGet { c =>
      c.updatedWith(acl.address)(_.map(_ ++ acl).orElse(Some(acl)))
    }.void

  def append(address: AclAddress, acl: (Identity, Set[Permission])*): IO[Unit] =
    append(Acl(address, acl.toMap))

  def replace(address: AclAddress, acl: (Identity, Set[Permission])*): IO[Unit] =
    cache.updateAndGet { _.updated(address, Acl(address, acl.toMap)) }.void

  def delete(address: AclAddress): IO[Unit] =
    cache.updateAndGet { _.removed(address) }.void

  def subtract(address: AclAddress, acl: (Identity, Set[Permission])*): IO[Unit] =
    cache.updateAndGet { c =>
      val newAcl = Acl(address, acl.toMap)
      c.updatedWith(address)(_.map(_ -- newAcl).orElse(Some(newAcl)))
    }.void
}

object AclSimpleCheck {

  private def emptyAclSimpleCheck: IO[AclSimpleCheck] = {
    Ref.of[IO, Map[AclAddress, Acl]](Map.empty).map { cache =>
      val aclCheck = AclCheck((address, permission, identities) =>
        address.ancestors.foldM(false) {
          case (false, address) =>
            cache.get.map {
              _.get(address).exists(_.hasPermission(identities, permission))

            }
          case (true, _)        => IO.pure(true)
        }
      )
      new AclSimpleCheck(cache) {
        override def authorizeForOr[E <: Throwable](
            path: AclAddress,
            permission: Permission,
            identities: Set[Identity]
        )(onError: => E): IO[Unit] =
          aclCheck.authorizeForOr(path, permission, identities)(onError)

        override def authorizeFor(path: AclAddress, permission: Permission, identities: Set[Identity]): IO[Boolean] =
          aclCheck.authorizeFor(path, permission, identities)

        override def authorizeForEveryOr[E <: Throwable](path: AclAddress, permissions: Set[Permission])(
            onError: => E
        )(implicit caller: Caller): IO[Unit] =
          aclCheck.authorizeForEveryOr(path, permissions)(onError)

        override def mapFilterOrRaise[A, B](
            values: immutable.Iterable[A],
            extractAddressPermission: A => (AclAddress, Permission),
            onAuthorized: A => B,
            onFailure: AclAddress => IO[Unit]
        )(implicit caller: Caller): IO[Set[B]] =
          aclCheck.mapFilterOrRaise(values, extractAddressPermission, onAuthorized, onFailure)

        override def mapFilterAtAddressOrRaise[A, B](
            values: immutable.Iterable[A],
            address: AclAddress,
            extractPermission: A => Permission,
            onAuthorized: A => B,
            onFailure: AclAddress => IO[Unit]
        )(implicit caller: Caller): IO[Set[B]] =
          aclCheck.mapFilterAtAddressOrRaise(values, address, extractPermission, onAuthorized, onFailure)
      }
    }
  }

  /**
    * Create an [[AclSimpleCheck]] and initializes it with the provided acls
    * @param input
    *   the acls to append to the checker
    * @return
    */
  def apply(input: (Identity, AclAddress, Set[Permission])*): IO[AclSimpleCheck] =
    emptyAclSimpleCheck.flatTap { checker =>
      input.toList
        .traverse { case (subject, address, permissions) =>
          checker append (address, (subject, permissions))
        }
    }

  def unsafe(input: (Identity, AclAddress, Set[Permission])*): AclSimpleCheck =
    apply(input*).unsafeRunSync()

}
