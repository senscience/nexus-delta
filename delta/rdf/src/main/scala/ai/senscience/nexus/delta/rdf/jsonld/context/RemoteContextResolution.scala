package ai.senscience.nexus.delta.rdf.jsonld.context

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.{ContextArray, ContextRemoteIri}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContext.StaticContext
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolutionError.RemoteContextNotFound
import ai.senscience.nexus.delta.rdf.syntax.jsonOpsSyntax
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json

trait RemoteContextResolution { self =>

  /**
    * Resolve a passed ''iri''.
    *
    * @return
    *   the expected Json payload response from the passed ''iri''
    */
  def resolve(iri: Iri): IO[RemoteContext]

  /**
    * From a given ''json'', resolve all its remote context IRIs.
    *
    * @return
    *   a Map where the keys are the IRIs resolved and the values the @context value from the payload of the resolved
    *   wrapped in an IO
    */
  final def apply(json: Json): IO[Map[Iri, RemoteContext]] = {

    def inner(
        ctx: Set[ContextValue],
        resolved: Map[Iri, RemoteContext] = Map.empty
    ): IO[Map[Iri, RemoteContext]] = {
      val uris: Set[Iri] = ctx.flatMap(remoteIRIs).diff(resolved.keySet)
      for {
        curResolved     <- uris.parUnorderedTraverse { uri => resolve(uri).map(uri -> _) }
        curResolvedMap   = curResolved.toMap
        accResolved      = curResolvedMap ++ resolved
        recurseResolved <- curResolvedMap.values.toSet.parUnorderedTraverse { context =>
                             inner(Set(context.value), accResolved)
                           }
      } yield recurseResolved.foldLeft(accResolved)(_ ++ _)
    }

    inner(json.contextValues)
  }

  private def remoteIRIs(ctxValue: ContextValue): Set[Iri] =
    ctxValue match {
      case ContextArray(vector)  => vector.collect { case ContextRemoteIri(uri) => uri }.toSet
      case ContextRemoteIri(uri) => Set(uri)
      case _                     => Set.empty
    }

  /**
    * Merges the current [[RemoteContextResolution]] with the passed ones
    */
  def merge(others: RemoteContextResolution*): RemoteContextResolution =
    (iri: Iri) => {
      val ios = self.resolve(iri) :: others.map(_.resolve(iri)).toList
      ios.tailRecM {
        case Nil          => IO.raiseError(RemoteContextNotFound(iri)) // that never happens
        case head :: tail => head.attempt.map(_.leftMap(_ => tail))
      }
    }
}

object RemoteContextResolution {

  /**
    * Helper method to construct a [[RemoteContextResolution]] .
    *
    * @param f
    *   a pair of [[Iri]] and the resolved [[ContextValue]]
    */
  final def fixed(f: (Iri, ContextValue)*): RemoteContextResolution =
    new RemoteContextResolution {
      private val map                                   = f.toMap
      override def resolve(iri: Iri): IO[RemoteContext] =
        map.get(iri) match {
          case Some(value) => IO.pure(StaticContext(iri, value))
          case None        => IO.raiseError(RemoteContextNotFound(iri))
        }
    }

  final def loadResources(
      contexts: Set[(Iri, String)]
  )(using loader: ClasspathResourceLoader): IO[RemoteContextResolution] = {
    contexts.toList
      .traverse { case (iri, path) =>
        ContextValue.fromFile(path).map(iri -> _)
      }
      .map { contexts => fixed(contexts*) }
  }

  final def loadResourcesUnsafe(
      contexts: Set[(Iri, String)]
  )(using loader: ClasspathResourceLoader): RemoteContextResolution = {
    import cats.effect.unsafe.implicits.*
    loadResources(contexts).unsafeRunSync()
  }

  /**
    * A remote context resolution that never resolves
    */
  final val never: RemoteContextResolution = (iri: Iri) => IO.raiseError(RemoteContextNotFound(iri))
}
