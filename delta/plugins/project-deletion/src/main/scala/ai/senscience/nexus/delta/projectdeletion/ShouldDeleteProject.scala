package ai.senscience.nexus.delta.projectdeletion

import ai.senscience.nexus.delta.projectdeletion.ProjectDeletionRunner.ProjectDeletionCandidate
import ai.senscience.nexus.delta.projectdeletion.model.ProjectDeletionConfig
import cats.effect.{Clock, IO}
import cats.syntax.all.*

import java.time.{Duration, Instant}
import concurrent.duration.DurationLong

object ShouldDeleteProject {

  private def diff(left: Instant, right: Instant) =
    Math.abs(left.toEpochMilli - right.toEpochMilli).millis

  def apply(
      config: ProjectDeletionConfig,
      lastEventTime: ProjectDeletionCandidate => IO[Option[Instant]],
      clock: Clock[IO]
  ): ProjectDeletionCandidate => IO[Boolean] = (pr: ProjectDeletionCandidate) => {

    def isIncluded(pr: ProjectDeletionCandidate): Boolean =
      config.includedProjects.exists(regex => regex.matches(pr.ref.toString))

    def notExcluded(pr: ProjectDeletionCandidate): Boolean =
      !config.excludedProjects.exists(regex => regex.matches(pr.ref.toString))

    def deletableDueToDeprecation(pr: ProjectDeletionCandidate): Boolean =
      config.deleteDeprecatedProjects && pr.deprecated

    def deletableDueToBeingIdle(pr: ProjectDeletionCandidate): IO[Boolean] =
      clock.realTimeInstant.flatMap { now =>
        (IO.pure(projectIsIdle(pr, now)), resourcesAreIdle(pr, now)).mapN(_ && _)
      }

    def projectIsIdle(pr: ProjectDeletionCandidate, now: Instant) =
      diff(now, pr.updatedAt).toSeconds > config.idleInterval.toSeconds

    // A project with no statistics is treated as not idle (we can't tell it's been inactive long enough).
    def resourcesAreIdle(pr: ProjectDeletionCandidate, now: Instant): IO[Boolean] =
      lastEventTime(pr).map(_.exists(_.isBefore(now.minus(Duration.ofMillis(config.idleInterval.toMillis)))))

    def alreadyDeleted(pr: ProjectDeletionCandidate): Boolean = pr.markedForDeletion

    def worthyOfDeletion(pr: ProjectDeletionCandidate): IO[Boolean] =
      (IO.pure(deletableDueToDeprecation(pr)), deletableDueToBeingIdle(pr)).mapN(_ || _)

    (
      IO.pure(isIncluded(pr)),
      IO.pure(notExcluded(pr)),
      IO.pure(!alreadyDeleted(pr)),
      worthyOfDeletion(pr)
    ).mapN(_ && _ && _ && _)
  }
}
