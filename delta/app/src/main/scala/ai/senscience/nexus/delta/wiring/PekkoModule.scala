package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.config.DescriptionConfig
import ai.senscience.nexus.delta.kernel.utils.IOFuture
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import cats.effect.{IO, Resource}
import com.typesafe.config.Config
import izumi.distage.model.definition.{Id, ModuleDef}
import org.apache.pekko.actor.{ActorSystem, BootstrapSetup}
import org.apache.pekko.http.cors.scaladsl.settings.CorsSettings
import org.apache.pekko.http.scaladsl.model.HttpMethods.*
import org.apache.pekko.http.scaladsl.model.headers.Location
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route}
import org.apache.pekko.stream.{Materializer, SystemMaterializer}

import scala.concurrent.duration.DurationInt

final class PekkoModule(implicit classLoader: ClassLoader) extends ModuleDef {

  make[ActorSystem].fromResource { (description: DescriptionConfig, config: Config) =>
    val make    = IO.delay(
      ActorSystem(
        description.fullName,
        BootstrapSetup().withConfig(config).withClassloader(classLoader)
      )
    )
    val release = (as: ActorSystem) => {
      IOFuture.defaultCancelable(IO(as.terminate()).timeout(15.seconds)).void
    }
    Resource.make(make)(release)
  }
  make[Materializer].from((as: ActorSystem) => SystemMaterializer(as).materializer)
  make[RejectionHandler].from { (cr: RemoteContextResolution @Id("aggregate"), ordering: JsonKeyOrdering) =>
    RdfRejectionHandler(cr, ordering)
  }
  make[ExceptionHandler].from {
    (cr: RemoteContextResolution @Id("aggregate"), ordering: JsonKeyOrdering, base: BaseUri) =>
      RdfExceptionHandler(cr, ordering, base)
  }
  make[CorsSettings].from { (config: Config) =>
    CorsSettings(config)
      .withAllowedMethods(List(GET, PUT, POST, PATCH, DELETE, OPTIONS, HEAD))
      .withExposedHeaders(List(Location.name))
  }

  make[Vector[Route]].from { (pluginsRoutes: Set[PriorityRoute]) =>
    pluginsRoutes.toVector.sorted.map(_.route)
  }

}
