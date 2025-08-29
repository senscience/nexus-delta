package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.stream.ProjectionErr.CouldNotFindPipeErr
import cats.syntax.all.*

trait PipeChainCompiler {

  def apply(pipeChain: PipeChain): Either[ProjectionErr, Operation]

  def validate(pipeChain: PipeChain): Either[ProjectionErr, Unit] = apply(pipeChain).void
}

object PipeChainCompiler {

  def apply(definitions: Set[PipeDef]): PipeChainCompiler = new PipeChainCompiler {

    private val pipes = definitions.foldLeft(Map.empty[PipeRef, PipeDef]) { case (acc, definition) =>
      acc + (definition.ref -> definition)
    }

    private def lookup(ref: PipeRef): Either[CouldNotFindPipeErr, PipeDef] =
      pipes.get(ref).toRight(CouldNotFindPipeErr(ref))

    override def apply(pipeChain: PipeChain): Either[ProjectionErr, Operation] =
      for {
        configured <- pipeChain.pipes.traverse { case (ref, cfg) =>
                        lookup(ref).flatMap(_.withJsonLdConfig(cfg))
                      }
        chained    <- Operation.merge(configured)
      } yield chained
  }

  def alwaysFail: PipeChainCompiler = new PipeChainCompiler {
    override def apply(pipeChain: PipeChain): Either[ProjectionErr, Operation] =
      Left(CouldNotFindPipeErr(pipeChain.pipes.head._1))
  }
}
