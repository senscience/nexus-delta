package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.config.DescriptionConfig
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.{PluginDescription, ServiceDescription}
import ai.senscience.nexus.delta.kernel.dependency.{ComponentDescription, ServiceDependency}
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.VersionRoutes.{emtyVersionBundle, VersionBundle}
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.marshalling.{HttpResponseFields, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, Name}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.version
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}

import scala.collection.immutable.Iterable

class VersionRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    main: ServiceDescription,
    plugins: List[PluginDescription],
    dependencies: List[ServiceDependency],
    env: Name
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  private def fullOrDegraded(implicit caller: Caller) = aclCheck.authorizeFor(AclAddress.Root, version.read).flatMap {
    case true  => dependencies.traverse(_.serviceDescription).map(VersionBundle(main, _, plugins, env))
    case false => IO.pure(emtyVersionBundle)
  }

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("version") {
        extractCaller { implicit caller =>
          (get & pathEndOrSingleSlash) {
            emit(fullOrDegraded)
          }
        }
      }
    }

}

object VersionRoutes {

  private val emtyVersionBundle = VersionBundle(
    ServiceDescription.unresolved("delta"),
    List.empty,
    List.empty,
    Name.unsafe("unknown")
  )

  final private[routes] case class VersionBundle(
      main: ServiceDescription,
      dependencies: Iterable[ServiceDescription],
      plugins: Iterable[PluginDescription],
      env: Name
  )

  private[routes] object VersionBundle {
    private def toMap(values: Iterable[ComponentDescription]): Map[String, String] =
      values.map(desc => desc.name -> desc.version).toMap

    implicit private val versionBundleEncoder: Encoder.AsObject[VersionBundle] =
      Encoder.encodeJsonObject.contramapObject { case VersionBundle(main, dependencies, plugins, env) =>
        JsonObject(
          main.name      -> main.version.asJson,
          "dependencies" -> toMap(dependencies).asJson,
          "plugins"      -> toMap(plugins).asJson,
          "environment"  -> env.asJson
        )
      }

    implicit val versionBundleJsonLdEncoder: JsonLdEncoder[VersionBundle] =
      JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.version))

    implicit val versionHttpResponseFields: HttpResponseFields[VersionBundle] = HttpResponseFields.defaultOk
  }

  /**
    * Constructs a [[VersionRoutes]]
    */
  final def apply(
      identities: Identities,
      aclCheck: AclCheck,
      plugins: List[PluginDescription],
      dependencies: List[ServiceDependency],
      cfg: DescriptionConfig
  )(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): VersionRoutes = {
    new VersionRoutes(
      identities,
      aclCheck,
      ServiceDescription(cfg.name.value, cfg.version),
      plugins,
      dependencies,
      cfg.env
    )
  }
}
