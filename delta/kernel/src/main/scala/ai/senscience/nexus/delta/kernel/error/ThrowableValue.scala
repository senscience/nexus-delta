package ai.senscience.nexus.delta.kernel.error

abstract class ThrowableValue extends Throwable { self =>
  override def fillInStackTrace(): Throwable = self
}
