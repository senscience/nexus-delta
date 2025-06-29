package ai.senscience.nexus.delta.plugins.compositeviews.stream

import ai.senscience.nexus.delta.plugins.compositeviews.stream.CompositeBranch.Run
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.ProjectionProgress
import cats.syntax.all.*

/**
  * Describes the overall indexing progress of a composite views
  * @param sources
  *   the offset reached by each source in each run
  * @param branches
  *   the progress for each projection branch
  */
final case class CompositeProgress(
    sources: Map[(Iri, Run), Offset],
    branches: Map[CompositeBranch, ProjectionProgress]
) {

  /**
    * Returns the offset for the given source for the main branch
    * @param source
    *   the source identifier
    */
  def sourceMainOffset(source: Iri): Option[Offset] = sources.get(source -> Run.Main)

  /**
    * Returns the offset for the given source for the rebuild branch
    * @param source
    *   the source identifier
    */
  def sourceRebuildOffset(source: Iri): Option[Offset] = sources.get(source -> Run.Rebuild)

  /**
    * Update the progress for the given branch
    */
  def update(branch: CompositeBranch, progress: ProjectionProgress): CompositeProgress = {
    val updatedBranches = branches.updated(branch, progress)
    val updatedSources  = sources.updatedWith(branch.source -> branch.run)(_.min(Some(progress.offset)))
    copy(sources = updatedSources, branches = updatedBranches)
  }

}

object CompositeProgress {

  /**
    * Construct a composite progress from the branches, deducing the source progress from them
    * @param branches
    *   the progress per branch
    */
  def apply(branches: Map[CompositeBranch, ProjectionProgress]): CompositeProgress =
    new CompositeProgress(
      branches.foldLeft(Map.empty[(Iri, Run), Offset]) { case (acc, (branch, branchProgress)) =>
        acc.updatedWith((branch.source, branch.run)) {
          case Some(sourceOffset) => Some(branchProgress.offset.min(sourceOffset))
          case None               => Some(branchProgress.offset)
        }
      },
      branches
    )

}
