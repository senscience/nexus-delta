package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.kernel.cache.{CacheConfig, LocalCache}
import ai.senscience.nexus.delta.sdk.acls.model.{AclAddress, FlattenedAclStore}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.trace.Tracer

import scala.collection.immutable.Iterable

trait AclCheck {

  /**
    * Checks whether a given [[Caller]] has the passed ''permission'' on the passed ''path'', raising the error
    * ''onError'' when it doesn't
    */
  def authorizeForOr[E <: Throwable](path: AclAddress, permission: Permission)(onError: => E)(using
      caller: Caller
  ): IO[Unit]

  /**
    * Checks whether a given [[Caller]] has the passed ''permission'' on the passed ''path''.
    */
  def authorizeFor(path: AclAddress, permission: Permission)(using caller: Caller): IO[Boolean]

  /**
    * Checks whether a given [[Caller]] has all the passed ''permissions'' on the passed ''path'', raising the error
    * ''onError'' when it doesn't
    */
  def authorizeForEveryOr[E <: Throwable](path: AclAddress, permissions: Set[Permission])(
      onError: => E
  )(using Caller): IO[Unit]

  /**
    * Map authorized values for the provided caller.
    *
    * @param values
    *   the list of couples address permission to check
    * @param extractAddressPermission
    *   Extract an acl address and permission from a value [[A]]
    * @param onAuthorized
    *   to map the value [[A]] to [[B]] if access is granted
    * @param onFailure
    *   to raise an error at the first unauthorized value
    */
  def mapFilterOrRaise[A, B](
      values: Iterable[A],
      extractAddressPermission: A => (AclAddress, Permission),
      onAuthorized: A => B,
      onFailure: AclAddress => IO[Unit]
  )(using Caller): IO[Set[B]]

  /**
    * Map authorized values for the provided caller while filtering out the unauthorized ones.
    *
    * @param values
    *   the values to work on
    * @param extractAddressPermission
    *   Extract an acl address and permission from a value [[A]]
    * @param onAuthorized
    *   to map the value [[A]] to [[B]] if access is granted
    */
  def mapFilter[A, B](
      values: Iterable[A],
      extractAddressPermission: A => (AclAddress, Permission),
      onAuthorized: A => B
  )(using Caller): IO[Set[B]] =
    mapFilterOrRaise(values, extractAddressPermission, onAuthorized, _ => IO.unit)

  /**
    * Map authorized values for the provided caller while filtering out the unauthorized ones.
    *
    * @param values
    *   the list of couples address permission to check
    * @param address
    *   the address to check for
    * @param extractPermission
    *   Extract an acl address and permission from a value [[A]]
    * @param onAuthorized
    *   to map the value [[A]] to [[B]] if access is granted
    */
  def mapFilterAtAddressOrRaise[A, B](
      values: Iterable[A],
      address: AclAddress,
      extractPermission: A => Permission,
      onAuthorized: A => B,
      onFailure: AclAddress => IO[Unit]
  )(using Caller): IO[Set[B]]

  /**
    * Map authorized values for the provided caller while filtering out the unauthorized ones.
    *
    * @param values
    *   the list of couples address permission to check
    * @param address
    *   the address to check for
    * @param extractPermission
    *   Extract an acl address and permission from a value [[A]]
    * @param onAuthorized
    *   to map the value [[A]] to [[B]] if access is granted
    */
  def mapFilterAtAddress[A, B](
      values: Iterable[A],
      address: AclAddress,
      extractPermission: A => Permission,
      onAuthorized: A => B
  )(using Caller): IO[Set[B]] =
    mapFilterAtAddressOrRaise(values, address, extractPermission, onAuthorized, _ => IO.unit)
}

object AclCheck {

  def apply(aclStore: FlattenedAclStore, cacheConfig: CacheConfig)(using Tracer[IO]): IO[AclCheck] = {
    if cacheConfig.enabled then
      LocalCache[(AclAddress, Permission, Subject), Boolean](cacheConfig).map { cache =>
        def cachedCheck(address: AclAddress, permission: Permission, caller: Caller) =
          cache.getOrElseUpdate((address, permission, caller.subject), aclStore.exists(address, permission, caller))
        apply(cachedCheck)
      }
    else IO.pure(apply(aclStore.exists))
  }

  def apply(checkAcl: (AclAddress, Permission, Caller) => IO[Boolean])(using Tracer[IO]): AclCheck =
    new AclCheck {

      def authorizeForOrFail[E <: Throwable](
          path: AclAddress,
          permission: Permission
      )(onError: => E)(using caller: Caller): IO[Unit] =
        authorizeFor(path, permission)
          .flatMap { result => IO.raiseUnless(result)(onError) }

      /**
        * Checks whether the provided entities have the passed ''permission'' on the passed ''path''.
        */
      override def authorizeFor(
          path: AclAddress,
          permission: Permission
      )(using caller: Caller): IO[Boolean] = checkAcl(path, permission, caller)
        .surround("authorizeFor")

      override def authorizeForOr[E <: Throwable](path: AclAddress, permission: Permission)(
          onError: => E
      )(using caller: Caller): IO[Unit] = authorizeForOrFail(path, permission)(onError)

      override def authorizeForEveryOr[E <: Throwable](path: AclAddress, permissions: Set[Permission])(onError: => E)(
          using caller: Caller
      ): IO[Unit] =
        permissions.toList
          .traverse { permission =>
            checkAcl(path, permission, caller).flatMap { result =>
              IO.raiseUnless(result)(onError)
            }
          }
          .void
          .surround("authorizeForEveryOr")

      override def mapFilterOrRaise[A, B](
          values: Iterable[A],
          extractAddressPermission: A => (AclAddress, Permission),
          onAuthorized: A => B,
          onFailure: AclAddress => IO[Unit]
      )(using caller: Caller): IO[Set[B]] =
        values.toList
          .foldLeftM(Set.empty[B]) { case (acc, value) =>
            val (address, permission) = extractAddressPermission(value)
            authorizeFor(address, permission).flatMap { success =>
              if success then IO.pure(acc + onAuthorized(value))
              else onFailure(address) >> IO.pure(acc)
            }
          }
          .surround("authorizeMapFilterOrRaise")

      def mapFilterAtAddressOrRaise[A, B](
          values: Iterable[A],
          address: AclAddress,
          extractPermission: A => Permission,
          onAuthorized: A => B,
          onFailure: AclAddress => IO[Unit]
      )(using caller: Caller): IO[Set[B]] =
        values.toList
          .foldLeftM(Set.empty[B]) { case (acc, value) =>
            val permission = extractPermission(value)
            authorizeFor(address, permission).flatMap { success =>
              if success then IO.pure(acc + onAuthorized(value))
              else onFailure(address) >> IO.pure(acc)
            }
          }
          .surround("authorizeMapFilterAtAddressOrRaise")
    }

}
