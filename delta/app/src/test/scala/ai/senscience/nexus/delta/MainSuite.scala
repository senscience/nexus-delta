package ai.senscience.nexus.delta

import ai.senscience.nexus.delta.plugin.PluginsLoader.PluginLoaderConfig
import ai.senscience.nexus.delta.sdk.plugin.PluginDef
import ai.senscience.nexus.delta.sourcing.postgres.{PostgresDb, PostgresPassword, PostgresUser}
import ai.senscience.nexus.delta.wiring.DeltaModule
import ai.senscience.nexus.testkit.config.SystemPropertyOverride
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.postgres.PostgresContainer
import cats.effect.IO
import com.typesafe.config.impl.ConfigImpl
import distage.Injector
import izumi.distage.model.PlannerInput
import izumi.distage.model.definition.{Module, ModuleDef}
import izumi.distage.model.plan.Roots
import izumi.distage.model.reflection.DIKey
import izumi.distage.planning.solver.PlanVerifier
import munit.catseffect.IOFixture
import munit.{AnyFixture, CatsEffectSuite}
import org.apache.pekko.http.scaladsl.server.Route
import org.testcontainers.containers.GenericContainer

import java.nio.file.{Files, Paths}
import scala.concurrent.duration.Duration

/**
  * Test class that allows to check that across core and plugins:
  *   - Plugins have been successfully assembled
  *   - HOCON configuration files match their classes counterpart
  *   - Distage wiring is valid
  */
class MainSuite extends NexusSuite with MainSuite.Fixture {

  override val munitIOTimeout: Duration = Duration(60, "s")

  private val pluginsParentPath  = Paths.get("target/plugins").toAbsolutePath
  private val pluginLoaderConfig = PluginLoaderConfig(pluginsParentPath.toString)

  override def munitFixtures: Seq[AnyFixture[?]] = List(main)

  test("ensure the plugin jar files have been copied correctly") {
    if Files.list(pluginsParentPath).toArray.length == 0 then
      fail(s"No plugin jar files were found in '$pluginsParentPath'")
  }

  test("yield a correct plan") {
    Main.loadPluginsAndConfig(pluginLoaderConfig).map { case (config, loader, pDefs) =>
      val pluginsInfoModule = new ModuleDef {
        make[List[PluginDef]].from(pDefs)
      }
      val modules: Module   =
        (DeltaModule(config, munitIORuntime, loader) :: pluginsInfoModule :: pDefs.map(_.module)).merge

      PlanVerifier()
        .verify[IO](
          bindings = modules,
          roots = Roots.Everything,
          providedKeys = Set.empty,
          excludedActivations = Set.empty
        )
        .throwOnError()
    }
  }

  test("load different configurations and create the object graph") {
    ConfigImpl.reloadSystemPropertiesConfig()
    Main
      .start(pluginLoaderConfig)
      .use { locator =>
        IO.delay(locator.get[Vector[Route]])
      }
      .void
  }

  // Diagnostic (kept ignored): plans the full graph, reports circular dependencies distage broke via
  // proxies, any residual cycles / self-loops, and dumps the whole dependency graph to target/distage-graph.dot.
  // Un-ignore and run `app/testOnly *MainSuite -- --tests=.*graph.*` to inspect the wiring.
  test("dump dependency graph and detect cycles/anomalies".ignore) {
    Main.loadPluginsAndConfig(pluginLoaderConfig).flatMap { case (config, loader, pDefs) =>
      val pluginsInfoModule = new ModuleDef {
        make[List[PluginDef]].from(pDefs)
      }
      val modules: Module   =
        (DeltaModule(config, munitIORuntime, loader) :: pluginsInfoModule :: pDefs.map(_.module)).merge

      IO.blocking {
        val injector  = Injector[IO]()
        val input     = PlannerInput.everything(modules)
        val plan      = injector.planUnsafe(input)
        val noRewrite = injector.planNoRewrite(input).getOrElse(plan)
        val dg        = noRewrite.plan
        val links     = dg.successors.links
        val nodes     = links.keySet ++ links.valuesIterator.flatten.toSet
        val edges     = links.valuesIterator.map(_.size).sum

        // proxy ops mark the cycles distage had to break at runtime
        val proxied = plan.plan.meta.nodes
          .collect {
            case (k, op) if op.getClass.getName.toLowerCase.contains("proxy") => k
          }
          .toList
          .sortBy(_.toString)

        // Kahn: strip in-degree-0 nodes; whatever remains sits on a residual cycle
        val indeg     = scala.collection.mutable.Map.from(nodes.iterator.map(_ -> 0))
        links.valuesIterator.foreach(_.foreach(b => indeg(b) += 1))
        val q         = scala.collection.mutable.Queue.from(nodes.iterator.filter(indeg(_) == 0))
        val removed   = scala.collection.mutable.Set.empty[DIKey]
        while q.nonEmpty do {
          val n = q.dequeue()
          removed += n
          links.getOrElse(n, Set.empty).foreach { m =>
            indeg(m) -= 1
            if indeg(m) == 0 then q.enqueue(m)
          }
        }
        val cyclic    = (nodes -- removed).toList.sortBy(_.toString)
        val selfLoops = links.collect { case (a, bs) if bs.contains(a) => a }.toList.sortBy(_.toString)

        val sb = new StringBuilder
        sb.append(s"nodes=${nodes.size} edges=$edges\n\n")
        sb.append(s"PROXY OPS (cycles distage broke via proxies): ${proxied.size}\n")
        proxied.foreach(k => sb.append(s"  $k\n"))
        sb.append(s"\nRESIDUAL CYCLIC NODES (Kahn): ${cyclic.size}\n")
        cyclic.foreach { k =>
          sb.append(s"  $k\n")
          links.getOrElse(k, Set.empty).intersect(cyclic.toSet).foreach(t => sb.append(s"      -> $t\n"))
        }
        sb.append(s"\nSELF-LOOPS: ${selfLoops.size}\n")
        selfLoops.foreach(k => sb.append(s"  $k\n"))

        // render the dependency tree of each proxied key to expose the actual cycle path
        proxied.filterNot(_.toString.contains("proxyinit")).foreach { k =>
          sb.append(s"\n--- dependency tree of $k (cycle marked by distage) ---\n")
          sb.append(plan.renderDeps(k))
          sb.append("\n")
        }

        val report = sb.toString
        println("=" * 90)
        println(report)
        println("=" * 90)

        // full dependency graph as DOT for offline inspection (dot -Tsvg target/distage-graph.dot)
        val dot          = new StringBuilder("digraph distage {\n  rankdir=LR;\n")
        def q2(k: DIKey) = "\"" + k.toString.replace("\"", "'") + "\""
        links.foreach { case (a, bs) => bs.foreach(b => dot.append(s"  ${q2(a)} -> ${q2(b)};\n")) }
        dot.append("}\n")
        Files.writeString(Paths.get("target/distage-graph.dot"), dot.toString)
        Files.writeString(Paths.get("target/distage-cycles.txt"), report)
        ()
      }
    }
  }

}

object MainSuite {

  trait Fixture { self: CatsEffectSuite =>

    // Overload config via system properties
    private def initConfig(postgres: GenericContainer[?]): IO[Map[String, String]] =
      IO.blocking {
        val resourceTypesFile = Files.createTempFile("resource-types", ".json")
        Files.writeString(resourceTypesFile, """["https://neuroshapes.org/Entity"]""")
        val mappingFile       = Files.createTempFile("mapping", ".json")
        Files.writeString(mappingFile, "{}")
        val queryFile         = Files.createTempFile("query", ".json")
        Files.writeString(
          queryFile,
          """CONSTRUCT { {resource_id} <https://schema.org/name> ?name } WHERE { {resource_id} <http://localhost/name> ?name }"""
        )

        Map(
          "app.defaults.database.access.host"            -> postgres.getHost,
          "app.defaults.database.access.port"            -> postgres.getMappedPort(5432).toString,
          "app.database.tables-autocreate"               -> "true",
          "app.defaults.database.access.username"        -> PostgresUser,
          "app.default.database.access.password"         -> PostgresPassword,
          "app.elasticsearch.indexing-enabled"           -> "false",
          "app.elasticsearch.credentials.type"           -> "anonymous",
          // TODO Investigate how to remove this property from the config
          "app.elasticsearch.disable-metrics-projection" -> "true",
          "plugins.graph-analytics.enabled"              -> "true",
          "plugins.graph-analytics.indexing-enabled"     -> "false",
          "plugins.search.enabled"                       -> "true",
          "plugins.search.indexing.resource-types"       -> resourceTypesFile.toString,
          "plugins.search.indexing.mapping"              -> mappingFile.toString,
          "plugins.search.indexing.query"                -> queryFile.toString,
          "otel.sdk.disabled"                            -> "true"
        )
      }

    // Start the necessary containers
    private def init() =
      PostgresContainer.resource(PostgresUser, PostgresPassword, PostgresDb).flatMap { postgres =>
        SystemPropertyOverride(initConfig(postgres))
      }

    val main: IOFixture[Unit] = ResourceSuiteLocalFixture("main", init())
  }

}
