package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.config.DescriptionConfig
import ai.senscience.nexus.delta.kernel.utils.IOFuture
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import akka.actor.{ActorSystem, BootstrapSetup}
import akka.http.scaladsl.model.HttpMethods.*
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route}
import akka.stream.{Materializer, SystemMaterializer}
import cats.effect.{IO, Resource}
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import com.typesafe.config.Config
import izumi.distage.model.definition.{Id, ModuleDef}

import scala.concurrent.duration.DurationInt

final class AkkaModule(implicit classLoader: ClassLoader) extends ModuleDef {

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
  make[CorsSettings].from(
    CorsSettings.defaultSettings
      .withAllowedMethods(List(GET, PUT, POST, PATCH, DELETE, OPTIONS, HEAD))
      .withExposedHeaders(List(Location.name))
  )

  make[Vector[Route]].from { (pluginsRoutes: Set[PriorityRoute]) =>
    pluginsRoutes.toVector.sorted.map(_.route)
  }

}
