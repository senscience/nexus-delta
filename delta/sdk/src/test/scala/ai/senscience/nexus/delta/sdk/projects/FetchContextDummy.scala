package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectIsDeprecated, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, Project, ProjectContext, ProjectRejection}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

class FetchContextDummy private (
    expected: Map[ProjectRef, ProjectContext],
    rejectOnCreate: Set[ProjectRef],
    rejectOnModify: Set[ProjectRef]
) extends FetchContext {

  override def defaultApiMappings: ApiMappings = ApiMappings.empty

  override def onRead(ref: ProjectRef): IO[ProjectContext] =
    IO.fromEither(expected.get(ref).toRight(ProjectNotFound(ref).asInstanceOf[ProjectRejection]))

  override def onCreate(ref: ProjectRef): IO[ProjectContext] =
    IO.raiseWhen(rejectOnCreate.contains(ref))(ProjectIsDeprecated(ref).asInstanceOf[ProjectRejection]) >> onRead(ref)

  override def onModify(ref: ProjectRef): IO[ProjectContext] =
    IO.raiseWhen(rejectOnModify.contains(ref))(ProjectIsDeprecated(ref).asInstanceOf[ProjectRejection]) >> onRead(ref)
}

object FetchContextDummy {

  val empty = new FetchContextDummy(Map.empty, Set.empty, Set.empty)

  def apply(
      expected: Map[ProjectRef, ProjectContext],
      rejectOnCreateOrModify: Set[ProjectRef]
  ): FetchContext =
    new FetchContextDummy(expected, rejectOnCreateOrModify, rejectOnCreateOrModify)

  def apply(expected: Map[ProjectRef, ProjectContext]): FetchContext =
    new FetchContextDummy(expected, Set.empty, Set.empty)

  def apply(expected: List[Project]): FetchContext =
    new FetchContextDummy(expected.map { p => p.ref -> p.context }.toMap, Set.empty, Set.empty)

  def apply(
      expected: Map[ProjectRef, ProjectContext],
      rejectOnCreate: Set[ProjectRef],
      rejectOnModify: Set[ProjectRef]
  ): FetchContext =
    new FetchContextDummy(expected, rejectOnCreate, rejectOnModify)

}
