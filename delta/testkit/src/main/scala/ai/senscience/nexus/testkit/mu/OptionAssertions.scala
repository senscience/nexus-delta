package ai.senscience.nexus.testkit.mu

import munit.{Assertions, Location}

trait OptionAssertions { self: Assertions =>

  extension [A](option: Option[A])(using Location) {

    def assertSome(value: A): Unit =
      assert(option.contains(value), s"$value was not found in the option, it contains '${option.getOrElse("None")}'")
    def assertNone(): Unit         =
      assert(
        option.isEmpty,
        s"The option is not empty, it contains ${option.getOrElse(new IllegalStateException("Should not happen))}."))}"
      )

    def assertSome(): Unit =
      assert(option.isDefined, "The option is empty.")
  }

}
