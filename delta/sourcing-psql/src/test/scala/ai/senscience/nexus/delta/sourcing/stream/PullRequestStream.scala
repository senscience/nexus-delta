package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.{PullRequestActive, PullRequestClosed}
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ch.epfl.bluebrain.nexus.delta.rdf.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.delta.rdf.syntax.*
import fs2.{Pure, Stream}

import java.time.Instant

object PullRequestStream {

  def generate(projectRef: ProjectRef): Stream[Pure, Elem[GraphResource]] = {
    val base    = iri"http://localhost"
    val instant = Instant.EPOCH

    val pr1 = PullRequestActive(
      id = nxv + "id1",
      project = projectRef,
      rev = 1,
      createdAt = instant,
      createdBy = Anonymous,
      updatedAt = instant,
      updatedBy = Anonymous
    )

    val pr2 = PullRequestClosed(
      id = nxv + "id2",
      project = projectRef,
      rev = 1,
      createdAt = instant,
      createdBy = Anonymous,
      updatedAt = instant,
      updatedBy = Anonymous
    )

    Stream(
      SuccessElem(
        tpe = PullRequest.entityType,
        id = pr1.id,
        project = projectRef,
        instant = pr1.updatedAt,
        offset = Offset.at(1L),
        value = PullRequestState.toGraphResource(pr1, base),
        rev = 1
      ),
      DroppedElem(
        tpe = PullRequest.entityType,
        id = nxv + "dropped",
        project = projectRef,
        Instant.EPOCH,
        Offset.at(2L),
        rev = 1
      ),
      SuccessElem(
        tpe = PullRequest.entityType,
        id = pr2.id,
        project = projectRef,
        instant = pr2.updatedAt,
        offset = Offset.at(3L),
        value = PullRequestState.toGraphResource(pr2, base),
        rev = 1
      ),
      FailedElem(
        tpe = PullRequest.entityType,
        id = nxv + "failed",
        project = projectRef,
        Instant.EPOCH,
        Offset.at(4L),
        new IllegalStateException("This is an error message"),
        rev = 1
      )
    )
  }

}
