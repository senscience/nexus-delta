package ai.senscience.nexus.delta.plugins.blazegraph.supervision

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.SparqlSupervision.SparqlNamespaceTriples
import ai.senscience.nexus.delta.sdk.views.ViewRef
import cats.effect.IO
import cats.kernel.Monoid
import cats.syntax.all.*
import io.circe.syntax.KeyOps
import io.circe.{Encoder, Json, JsonObject}

/**
  * Gives supervision information for the underlying SPARQL instance
  */
trait SparqlSupervision {
  def get: IO[SparqlNamespaceTriples]
}

object SparqlSupervision {

  /**
    * Returns the number of triples
    * @param total
    *   the total number of triples in the SPARQL instances
    * @param assigned
    *   the triples per SPARQL views
    * @param unassigned
    *   the triples for namespaces which can not be associated to a SPARQL view
    */
  final case class SparqlNamespaceTriples(
      total: Long,
      assigned: Map[ViewRef, Long],
      unassigned: Map[String, Long]
  )

  object SparqlNamespaceTriples {
    val empty: SparqlNamespaceTriples = SparqlNamespaceTriples(0L, Map.empty, Map.empty)

    implicit val sparqlNamespaceTriplesMonoid: Monoid[SparqlNamespaceTriples] = new Monoid[SparqlNamespaceTriples] {
      override def empty: SparqlNamespaceTriples = SparqlNamespaceTriples.empty

      override def combine(x: SparqlNamespaceTriples, y: SparqlNamespaceTriples): SparqlNamespaceTriples =
        SparqlNamespaceTriples(
          x.total + y.total,
          x.assigned ++ y.assigned,
          x.unassigned ++ y.unassigned
        )
    }

    implicit final val sparqlNamespacesEncoder: Encoder[SparqlNamespaceTriples] = Encoder.AsObject.instance { value =>
      val assigned = value.assigned.toVector.sortBy(_._1.toString).map { case (view, count) =>
        Json.obj("project" := view.project, "view" := view.viewId, "count" := count)
      }

      val unassigned = value.unassigned.toVector.sortBy(_._1).map { case (namespace, count) =>
        Json.obj("namespace" := namespace, "count" := count)
      }

      JsonObject(
        "total"      := value.total,
        "assigned"   := Json.arr(assigned*),
        "unassigned" := Json.arr(unassigned*)
      )
    }
  }

  def apply(client: SparqlClient, viewsByNamespace: ViewByNamespace): SparqlSupervision =
    new SparqlSupervision {
      override def get: IO[SparqlNamespaceTriples] = {
        IO.both(client.listNamespaces, viewsByNamespace.get)
          .flatMap { case (namespaces, viewsByNamespace) =>
            namespaces.parFoldMapA { namespace =>
              client.count(namespace).map { count =>
                viewsByNamespace.get(namespace) match {
                  case Some(view) => SparqlNamespaceTriples(count, Map(view -> count), Map.empty)
                  case None       => SparqlNamespaceTriples(count, Map.empty, Map(namespace -> count))
                }
              }
            }
          }
      }
    }
}
