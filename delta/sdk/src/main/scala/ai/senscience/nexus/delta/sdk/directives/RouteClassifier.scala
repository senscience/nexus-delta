package ai.senscience.nexus.delta.sdk.directives

import org.apache.pekko.http.scaladsl.model.Uri

import scala.annotation.tailrec

/**
  * A low-cardinality span name, the same shape used today, e.g. `"schemas/<str:org>/<str:project>/<str:id>"`. The
  * request method is not part of it: it is prepended when naming the span (e.g.
  * `GET schemas/<str:org>/<str:project>/<str:id>`), and the same value is also recorded as `http.route`.
  */
opaque type SpanName = String

object SpanName {
  def apply(value: String): SpanName = value

  extension (spanName: SpanName) def value: String = spanName
}

/**
  * Maps a request path to a [[SpanName]], or `None` for paths that should not be traced (health checks, or any path
  * with no matching route).
  *
  * A classifier is built from nested [[RouteClassifier.route]] nodes that mirror the route tree: a common prefix is
  * declared once and each nested route only adds its own segment. A route both names a span (for a path ending there)
  * and may hold nested sub-routes. The span name is derived from the path itself, so it never drifts from the shape:
  * {{{
  *   import RouteClassifier.*
  *
  *   RouteClassifier(
  *     route("resources" / str("org") / str("project"))(   // resources/<str:org>/<str:project>
  *       route(str("schema"))(                              // .../<str:schema>
  *         route(str("id"))(                                // .../<str:id>
  *           route("tags"),                                 // .../<str:id>/tags
  *           route("source")                                // .../<str:id>/source
  *         )
  *       )
  *     )
  *   )
  * }}}
  *
  * A path fragment is written with string literals and [[RouteClassifier.str]] captures joined by `/`; a bare literal
  * (`"resources"`, `"tags"`) is accepted directly by both `route` and `/`.
  *
  * Classifiers compose via [[orElse]] (and [[RouteClassifier.combine]]), so each module/plugin contributes its own,
  * mirroring how routes themselves are assembled. The first matching route wins.
  *
  * Note: the classifier sees whatever path it is given. If applied above the base-uri prefix stripping, the top routes
  * must account for the leading segment(s) (or the caller should strip them first).
  */
final class RouteClassifier private (private val pf: PartialFunction[List[String], SpanName]) {

  /** The span name for the given path, or `None` if no route matches (i.e. the request is not traced). */
  def apply(path: Uri.Path): Option[SpanName] =
    pf.lift(RouteClassifier.segments(path))

  /** A classifier that tries `this` first, then `that`. */
  def orElse(that: RouteClassifier): RouteClassifier =
    new RouteClassifier(pf.orElse(that.pf))
}

object RouteClassifier {

  /** A path segment: a fixed literal, or a named capture rendering as `<tpe:name>` and matching any single segment. */
  enum Segment {
    case Lit(value: String)
    case Capture(tpe: String, name: String)
  }

  /** A path fragment — a sequence of segments, built from literals and captures joined by `/`. */
  opaque type Path = Vector[Segment]

  extension (segment: String) {

    /** Starts a fragment from a literal followed by another fragment, e.g. `"resources" / str("org")`. */
    def /(next: Path): Path = Segment.Lit(segment) +: next

    /** Starts a fragment from two literals, e.g. `"trial" / "resources"`. */
    def /(next: String): Path = Vector(Segment.Lit(segment), Segment.Lit(next))
  }

  extension (path: Path) {

    /** Appends another fragment, e.g. `str("org") / str("project")`. */
    def /(next: Path): Path = path ++ next

    /** Appends a literal, e.g. `str("id") / "source"`. */
    def /(next: String): Path = path :+ Segment.Lit(next)

    private def render: String =
      path
        .map {
          case Segment.Lit(value)         => value
          case Segment.Capture(tpe, name) => s"<$tpe:$name>"
        }
        .mkString("/")

    /** Consumes this fragment off the front of `segments`, returning the leftover, or `None` if it doesn't match. */
    private def consume(segments: List[String]): Option[List[String]] = {
      @tailrec
      def loop(fragment: List[Segment], remaining: List[String]): Option[List[String]] =
        (fragment, remaining) match {
          case (Nil, rest)                             => Some(rest)
          case (Segment.Lit(v) :: t, s :: r) if s == v => loop(t, r)
          case (Segment.Capture(_, _) :: t, _ :: r)    => loop(t, r)
          case _                                       => None
        }
      loop(path.toList, segments)
    }
  }

  /** A captured string segment, rendering as `<str:name>` and matching any single segment. */
  def str(name: String): Path = Vector(Segment.Capture("str", name))

  /** A route: a path fragment that names a span (for a path ending there), optionally holding nested sub-routes. */
  final case class RouteNode private[RouteClassifier] (path: Path, children: Vector[RouteNode])

  /** A leaf route — a path fragment with no nested sub-routes. */
  def route(path: Path): RouteNode = RouteNode(path, Vector.empty)

  /** A leaf route from a single literal segment, e.g. `route("undeprecate")`. */
  def route(segment: String): RouteNode = RouteNode(Vector(Segment.Lit(segment)), Vector.empty)

  /** A route with nested sub-routes for the paths that extend it. */
  def route(path: Path)(children: RouteNode*): RouteNode = RouteNode(path, children.toVector)

  /** A route from a single literal segment, with nested sub-routes, e.g. `route("_")(...)`. */
  def route(segment: String)(children: RouteNode*): RouteNode =
    RouteNode(Vector(Segment.Lit(segment)), children.toVector)

  /** Builds a classifier from the given top-level routes. */
  def apply(roots: RouteNode*): RouteClassifier =
    new RouteClassifier(Function.unlift { segments =>
      roots.iterator.flatMap(matchNode(_, Vector.empty, segments)).nextOption()
    })

  /** A classifier that matches nothing (every request is untraced). Identity for [[combine]]. */
  val empty: RouteClassifier = new RouteClassifier(PartialFunction.empty)

  /** Combines classifiers, earlier ones taking precedence. */
  def combine(classifiers: IterableOnce[RouteClassifier]): RouteClassifier =
    classifiers.iterator.foldLeft(empty)(_.orElse(_))

  private def matchNode(node: RouteNode, prefix: Path, remaining: List[String]): Option[SpanName] =
    node.path.consume(remaining).flatMap { rest =>
      val here = prefix / node.path
      if rest.isEmpty then Some(SpanName(here.render))
      else node.children.iterator.flatMap(matchNode(_, here, rest)).nextOption()
    }

  /** The non-empty path segments, slashes dropped, e.g. `/v1/schemas/org` -> `List("v1", "schemas", "org")`. */
  def segments(path: Uri.Path): List[String] = {
    @tailrec
    def loop(remaining: Uri.Path, acc: List[String]): List[String] =
      remaining match {
        case Uri.Path.Empty               => acc.reverse
        case Uri.Path.Slash(tail)         => loop(tail, acc)
        case Uri.Path.Segment(head, tail) => loop(tail, head :: acc)
      }
    loop(path, Nil)
  }
}
