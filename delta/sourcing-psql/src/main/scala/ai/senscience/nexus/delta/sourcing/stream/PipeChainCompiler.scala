package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.stream.ProjectionErr.CouldNotFindPipeErr
import cats.syntax.all.*

trait PipeChainCompiler {

  def apply(pipeChain: PipeChain): Either[ProjectionErr, Operation]

  def validate(pipeChain: PipeChain): Either[ProjectionErr, Unit] = apply(pipeChain).void
}

object PipeChainCompiler {

  def apply(registry: ReferenceRegistry): PipeChainCompiler = (pipeChain: PipeChain) =>
    for {
      configured <- pipeChain.pipes.traverse { case (ref, cfg) =>
                      registry.lookup(ref).flatMap(_.withJsonLdConfig(cfg))
                    }
      chained    <- Operation.merge(configured)
    } yield chained

  def alwaysFail: PipeChainCompiler = new PipeChainCompiler {
    override def apply(pipeChain: PipeChain): Either[ProjectionErr, Operation] =
      Left(CouldNotFindPipeErr(pipeChain.pipes.head._1))
  }
}
