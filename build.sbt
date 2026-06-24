import java.time.Instant
import scala.io.Source

// explicit import to avoid clash with gatling plugin
import sbtassembly.AssemblyPlugin.autoImport.assembly

/*
scalafmt: {
  maxColumn = 150
  align.tokens."+" = [
    { code = ":=", owner = "Term.ApplyInfix" }
    { code = "+=", owner = "Term.ApplyInfix" }
    { code = "++=", owner = "Term.ApplyInfix" }
    { code = "~=", owner = "Term.ApplyInfix" }
  ]
}
 */

val scalaCompilerVersion     = "3.8.4"
val typelevelScalafixVersion = "0.5.0"

val awsSdkVersion              = "2.46.14"
val caffeineVersion            = "3.2.4"
val catsEffectVersion          = "3.7.0"
val catsRetryVersion           = "4.0.0"
val catsVersion                = "2.13.0"
val circeVersion               = "0.14.15"
val circeOpticsVersion         = "0.15.1"
val circeExtrasVersions        = "0.14.5-RC1"
val classgraphVersion          = "4.8.184"
val distageVersion             = "1.2.25"
val doobieVersion              = "1.0.0-RC13"
val fs2Version                 = "3.13.0"
val fs2AwsVersion              = "6.4.0"
val gatlingVersion             = "3.15.1"
val glassFishJakartaVersion    = "2.0.1"
val handleBarsVersion          = "4.5.2"
val hikariVersion              = "7.1.0"
val http4sVersion              = "0.23.34"
val http4sXMLVersion           = "0.24.0"
val jenaVersion                = "6.1.0"
val jsonIterVersion            = "2.38.15"
val log4catsVersion            = "2.8.0"
val logbackVersion             = "1.5.34"
val magnoliaVersion            = "1.3.21"
val munitVersion               = "1.3.3"
val munitCatsEffectVersion     = "2.2.0"
val nimbusJoseJwtVersion       = "10.9.1"
val otelVersion                = "1.61.0"
val otel4sVersion              = "1.0.0"
val otelInstrumentationVersion = "2.27.0-alpha"
val pekkoVersion               = "1.6.0"
val pekkoConnectorsVersion     = "1.3.0"
val pekkoHttpVersion           = "1.3.0"
val postgresJdbcVersion        = "42.7.11"
val pureconfigVersion          = "0.17.10"
val scalacCompatVersion        = "0.1.4"
val scalaTestVersion           = "3.2.20"
val scalaXmlVersion            = "2.4.0"
val shapeless3Version          = "3.6.0"
val titaniumJsonLdVersion      = "1.7.0"
val testContainersVersion      = "2.0.5"
val testContainersScalaVersion = "0.44.1"

lazy val awsSdk             = "software.amazon.awssdk"        % "s3"                   % awsSdkVersion
lazy val caffeine           = "com.github.ben-manes.caffeine" % "caffeine"             % caffeineVersion
lazy val catsCore           = "org.typelevel"                %% "cats-core"            % catsVersion
lazy val catsEffect         = "org.typelevel"                %% "cats-effect"          % catsEffectVersion
lazy val catsRetry          = "com.github.cb372"             %% "cats-retry"           % catsRetryVersion
lazy val circeCore          = "io.circe"                     %% "circe-core"           % circeVersion
lazy val circeGeneric       = "io.circe"                     %% "circe-generic"        % circeVersion
lazy val circeGenericExtras = "io.circe"                     %% "circe-generic-extras" % circeExtrasVersions
lazy val circeLiteral       = "io.circe"                     %% "circe-literal"        % circeVersion
lazy val circeOptics        = "io.circe"                     %% "circe-optics"         % circeOpticsVersion
lazy val circeParser        = "io.circe"                     %% "circe-parser"         % circeVersion
lazy val classgraph         = "io.github.classgraph"          % "classgraph"           % classgraphVersion
lazy val distageCore        = "io.7mind.izumi"               %% "distage-core"         % distageVersion
lazy val doobiePostgres     = "org.typelevel"                %% "doobie-postgres"      % doobieVersion
lazy val doobie             = Seq(
  "org.typelevel" %% "doobie-postgres" % doobieVersion,
  "org.typelevel" %% "doobie-hikari"   % doobieVersion,
  "org.typelevel" %% "doobie-otel4s"   % doobieVersion,
  "com.zaxxer"     % "HikariCP"        % hikariVersion exclude ("org.slf4j", "slf4j-api"),
  "org.postgresql" % "postgresql"      % postgresJdbcVersion
)
lazy val fs2                = "co.fs2"                       %% "fs2-core"             % fs2Version
lazy val fs2io              = "co.fs2"                       %% "fs2-io"               % fs2Version

lazy val fs2Aws = Seq(
  "co.fs2"       %% "fs2-reactive-streams" % fs2Version,
  "io.laserdisc" %% "fs2-aws-core"         % fs2AwsVersion,
  "io.laserdisc" %% "fs2-aws-s3"           % fs2AwsVersion
).map {
  _.excludeAll(
    ExclusionRule(organization = "org.typelevel", name = "cats-kernel_3"),
    ExclusionRule(organization = "org.typelevel", name = "cats-core_3"),
    ExclusionRule(organization = "org.typelevel", name = "cats-effect_3"),
    ExclusionRule(organization = "com.chuusai", name = "shapeless3_3"),
    ExclusionRule(organization = "co.fs2", name = "fs2-core_3"),
    ExclusionRule(organization = "co.fs2", name = "fs2-io_3")
  )
}

lazy val scalacCompat = "org.typelevel" %% "scalac-compat-annotation" % scalacCompatVersion

lazy val gatling = Seq(
  "io.gatling.highcharts" % "gatling-charts-highcharts" % gatlingVersion % "test,it",
  "io.gatling"            % "gatling-test-framework"    % gatlingVersion % "test,it"
)

lazy val glassFishJakarta = "org.glassfish"     % "jakarta.json" % glassFishJakartaVersion
lazy val handleBars       = "com.github.jknack" % "handlebars"   % handleBarsVersion

// Used only for tests for now
lazy val http4sServerTest = Seq(
  "org.http4s" %% "http4s-ember-server" % http4sVersion % Test,
  "org.http4s" %% "http4s-dsl"          % http4sVersion % Test
)

lazy val http4s = Seq(
  "org.http4s" %% "http4s-ember-client" % http4sVersion,
  "org.http4s" %% "http4s-scala-xml"    % http4sXMLVersion
) ++ http4sServerTest

lazy val jenaArq   = "org.apache.jena" % "jena-arq"   % jenaVersion
lazy val jenaShacl = "org.apache.jena" % "jena-shacl" % jenaVersion

lazy val jsoniterCirce = "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-circe" % jsonIterVersion

lazy val log4cats        = "org.typelevel"                %% "log4cats-slf4j"    % log4catsVersion
lazy val logback         = "ch.qos.logback"                % "logback-classic"   % logbackVersion
lazy val magnolia        = "com.softwaremill.magnolia1_3" %% "magnolia"          % magnoliaVersion
lazy val munit           = "org.scalameta"                %% "munit"             % munitVersion
lazy val munitCatsEffect = "org.typelevel"                %% "munit-cats-effect" % munitCatsEffectVersion
lazy val nimbusJoseJwt   = "com.nimbusds"                  % "nimbus-jose-jwt"   % nimbusJoseJwtVersion

lazy val otel4s        = "org.typelevel" %% "otel4s-oteljava"                 % otel4sVersion
lazy val otel4sStorage = "org.typelevel" %% "otel4s-oteljava-context-storage" % otel4sVersion
lazy val otel4sSemconv = "org.typelevel" %% "otel4s-semconv"                  % otel4sVersion
lazy val otel4sTestkit = "org.typelevel" %% "otel4s-oteljava-testkit"         % otel4sVersion

// OpenTelemetry Java SQL analyzer, used to name doobie DB spans by query summary (e.g. `SELECT public.scoped_states`)
lazy val otelSqlAnalyzer =
  "io.opentelemetry.instrumentation" % "opentelemetry-instrumentation-api-incubator" % otelInstrumentationVersion
lazy val otelHikari = "io.opentelemetry.instrumentation" % "opentelemetry-hikaricp-3.0" % otelInstrumentationVersion

lazy val otelAutoconfigure = "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % otelVersion % Runtime
lazy val otelExporterOtlp  = "io.opentelemetry" % "opentelemetry-exporter-otlp"               % otelVersion % Runtime
lazy val otelDependencies  = Seq(
  "io.opentelemetry.instrumentation" % "opentelemetry-logback-appender-1.0" % otelInstrumentationVersion,
  "io.opentelemetry.instrumentation" % "opentelemetry-logback-mdc-1.0"      % otelInstrumentationVersion,
  "io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry"    % otelInstrumentationVersion,
  "org.typelevel"                   %% "otel4s-instrumentation-metrics"     % otel4sVersion
)

lazy val pekkoHttp        = "org.apache.pekko" %% "pekko-http"         % pekkoHttpVersion
lazy val pekkoHttpCore    = "org.apache.pekko" %% "pekko-http-core"    % pekkoHttpVersion
lazy val pekkoHttpCors    = "org.apache.pekko" %% "pekko-http-cors"    % pekkoHttpVersion
lazy val pekkoHttpTestKit = "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpVersion
lazy val pekkoHttpXml     = "org.apache.pekko" %% "pekko-http-xml"     % pekkoHttpVersion

lazy val pekkoSlf4j          = "org.apache.pekko" %% "pekko-slf4j"           % pekkoVersion
lazy val pekkoStream         = "org.apache.pekko" %% "pekko-stream"          % pekkoVersion
lazy val pekkoTestKit        = "org.apache.pekko" %% "pekko-testkit"         % pekkoVersion
lazy val pekkoConnectorsFile = "org.apache.pekko" %% "pekko-connectors-file" % pekkoConnectorsVersion excludeAll (
  ExclusionRule(organization = "org.apache.pekko", name = "pekko-stream_3")
)
lazy val pekkoConnectorsSse  = "org.apache.pekko" %% "pekko-connectors-sse"  % pekkoConnectorsVersion excludeAll (
  ExclusionRule(organization = "org.apache.pekko", name = "pekko-stream_3"),
  ExclusionRule(organization = "org.apache.pekko", name = "pekko-http_3")
)

lazy val pureConfig = Seq(
  "com.github.pureconfig" %% "pureconfig-core"           % pureconfigVersion,
  "com.github.pureconfig" %% "pureconfig-cats"           % pureconfigVersion,
  "com.github.pureconfig" %% "pureconfig-http4s"         % pureconfigVersion,
  "com.github.pureconfig" %% "pureconfig-generic-scala3" % pureconfigVersion
)

lazy val scalaTest                     = "org.scalatest"          %% "scalatest"                          % scalaTestVersion
lazy val scalaXml                      = "org.scala-lang.modules" %% "scala-xml"                          % scalaXmlVersion
lazy val shapeless3Typeable            = "org.typelevel"          %% "shapeless3-typeable"                % shapeless3Version
lazy val titaniumJsonLd                = "com.apicatalog"          % "titanium-json-ld"                   % titaniumJsonLdVersion
lazy val testContainers                = "org.testcontainers"      % "testcontainers"                     % testContainersVersion
lazy val testContainersScala           = "com.dimafeng"           %% "testcontainers-scala-munit"         % testContainersScalaVersion
lazy val testContainersScalaLocalStack = "com.dimafeng"           %% "testcontainers-scala-localstack-v2" % testContainersScalaVersion

val javaSpecificationVersion = SettingKey[String](
  "java-specification-version",
  "The java specification version to be used for source and target compatibility."
)

lazy val checkJavaVersion = taskKey[Unit]("Verifies the current Java version is compatible with the code java version")

lazy val copyPlugins = taskKey[Unit]("Assembles and copies the plugin files plugins directory")

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxMaterialThemePlugin, SitePreviewPlugin, ParadoxSitePlugin)
  .settings(shared, compilation, assertJavaVersion, noPublish)
  .settings(ParadoxMaterialThemePlugin.paradoxMaterialThemeSettings)
  .settings(
    name                             := "docs",
    moduleName                       := "docs",
    // paradox settings
    paradoxValidationIgnorePaths    ++= {
      val source      = Source.fromFile(file("docs/ignore-paths.txt"))
      val ignorePaths = source.getLines().map(_.r).toList
      source.close()
      ignorePaths
    },
    Compile / paradoxMaterialTheme   := {
      ParadoxMaterialTheme()
        .withColor("light-blue", "cyan")
        .withFavicon("./assets/img/favicon-32x32.png")
        .withLogo("./assets/img/logo.png")
        .withCustomStylesheet("./assets/css/docs.css")
        .withRepository(uri("https://github.com/senscience/nexus-delta"))
        .withSocial(
          uri("https://github.com/senscience"),
          uri("https://github.com/senscience/nexus-delta/discussions")
        )
        .withCopyright(s"""Nexus is Open Source and available under the Apache 2 License.<br/>
                         |© 2017-2024 <a href="https://epfl.ch/">EPFL</a>
                         | <a href="https://bluebrain.epfl.ch/">The Blue Brain Project</a><br/>
                         |© 2025 <a href="https://www.senscience.ai/">SENSCIENCE</a>
                         |""".stripMargin)
    },
    Compile / paradoxNavigationDepth := 4,
    Compile / paradoxProperties     ++=
      Map(
        "github.base_url"       -> "https://github.com/senscience/nexus-delta/tree/master",
        "project.version.short" -> "Snapshot",
        "current.url"           -> "https://senscience.github.io/nexus-delta/docs/",
        "version.snapshot"      -> "true",
        "git.branch"            -> "master"
      ),
    paradoxRoots                     := List("docs/index.html"),
    previewPath                      := "docs/index.html",
    previewFixedPort                 := Some(4001),
    // copy contexts
    makeSite / mappings             ++= {
      def fileSources(base: File): Seq[File] = (base * "*.json").get
      val contextDirs                        =
        (Compile / resourceDirectory).all(ScopeFilter(inProjects(contextProjects *))).value.map(_ / "contexts")
      contextDirs.flatMap { dir =>
        fileSources(dir).map(file => file -> s"contexts/${file.getName}")
      }
    }
  )

lazy val pekkoMarshalling = project
  .in(file("pekko/marshalling"))
  .settings(name := "pekko-marshalling", moduleName := "pekko-marshalling")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      pekkoStream,
      pekkoHttp,
      circeCore,
      circeParser,
      jsoniterCirce,
      circeGenericExtras % Test,
      circeLiteral       % Test,
      scalaTest          % Test,
      pekkoTestKit       % Test,
      pekkoHttpTestKit   % Test
    )
  )

lazy val pekkoTestArchive = project
  .in(file("pekko/test-archive"))
  .settings(name := "pekko-test-archive", moduleName := "pekko-test-archive")
  .settings(commonSettings)
  .settings(
    coverageMinimumStmtTotal := 0,
    libraryDependencies     ++= Seq(
      pekkoStream,
      pekkoConnectorsFile,
      circeCore,
      circeParser,
      scalaTest
    )
  )

lazy val pekko = project
  .in(file("pekko"))
  .settings(shared, compilation, noPublish)
  .aggregate(pekkoMarshalling, pekkoTestArchive)

lazy val kernel = project
  .in(file("delta/kernel"))
  .settings(name := "delta-kernel", moduleName := "delta-kernel")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      caffeine,
      catsCore,
      catsRetry,
      catsEffect,
      circeCore,
      circeParser,
      circeLiteral,
      circeGenericExtras,
      fs2,
      fs2io,
      jsoniterCirce,
      handleBars,
      nimbusJoseJwt,
      log4cats,
      otel4s,
      otel4sSemconv,
      otel4sStorage,
      munit           % Test,
      munitCatsEffect % Test
    ) ++ pureConfig ++ http4s
  )

lazy val testkit = project
  .dependsOn(kernel)
  .in(file("delta/testkit"))
  .settings(name := "delta-testkit", moduleName := "delta-testkit")
  .settings(commonSettings)
  .settings(
    coverageMinimumStmtTotal := 0,
    libraryDependencies     ++= Seq(
      logback,
      munit,
      munitCatsEffect,
      scalaTest,
      testContainers,
      testContainersScala,
      testContainersScalaLocalStack
    ) ++ doobie ++ fs2Aws
  )

lazy val sourcingPsql = project
  .in(file("delta/sourcing-psql"))
  .dependsOn(rdf, testkit % "test->compile")
  .settings(
    name       := "delta-sourcing-psql",
    moduleName := "delta-sourcing-psql"
  )
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      classgraph,
      distageCore,
      otelHikari,
      otelSqlAnalyzer,
      shapeless3Typeable
    ) ++ doobie,
    Test / fork          := true
  )

lazy val rdf = project
  .in(file("delta/rdf"))
  .dependsOn(kernel, testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    name       := "delta-rdf",
    moduleName := "delta-rdf"
  )
  .settings(
    libraryDependencies ++= Seq(
      catsCore,
      glassFishJakarta,
      jenaArq,
      jenaShacl,
      magnolia,
      titaniumJsonLd
    ),
    Test / fork          := true
  )

lazy val sdk = project
  .in(file("delta/sdk"))
  .settings(
    name       := "delta-sdk",
    moduleName := "delta-sdk"
  )
  .dependsOn(kernel, pekkoMarshalling, sourcingPsql % "compile->compile;test->test", rdf % "compile->compile;test->test", testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    // OTel metric/span recording reads the current context via otel4s' IOLocal-backed context storage, which (like
    // the Delta app, see Main.scala) requires fiber-context tracking to be enabled in the JVM.
    Test / fork          := true,
    Test / javaOptions   += "-Dcats.effect.trackFiberContext=true",
    libraryDependencies ++= Seq(
      pekkoHttpXml exclude ("org.scala-lang.modules", "scala-xml_3"),
      scalaXml,
      distageCore,
      pekkoSlf4j       % Test,
      pekkoTestKit     % Test,
      pekkoHttpTestKit % Test,
      otel4sTestkit    % Test
    ) ++ http4s
  )

lazy val elasticsearch = project
  .in(file("delta/elasticsearch"))
  .settings(commonSettings)
  .dependsOn(
    sdk % "compile->compile;test->test"
  )
  .settings(
    Test / fork := true
  )

lazy val app = project
  .in(file("delta/app"))
  .settings(
    name       := "delta-app",
    moduleName := "delta-app"
  )
  .enablePlugins(UniversalPlugin, JavaAppPackaging, DockerPlugin, BuildInfoPlugin)
  .settings(commonSettings, servicePackaging, otelSettings)
  .dependsOn(sdk % "compile->compile;test->test", elasticsearch, testkit % "test->compile")
  .settings(Test / compile := (Test / compile).dependsOn(testPlugin / assembly).value)
  .settings(
    libraryDependencies  ++= Seq(
      pekkoHttpCors,
      pekkoSlf4j,
      classgraph,
      logback
    ),
    run / fork            := true,
    buildInfoKeys         := Seq[BuildInfoKey](
      version,
      BuildInfoKey.action("builtAt") { Instant.now }
    ),
    buildInfoPackage      := "ai.senscience.nexus.delta.config",
    Docker / packageName  := "nexus-delta",
    copyPlugins           := {
      val pluginFiles   = assembly.all(ScopeFilter(inProjects(pluginProjects *))).value
      val pluginsTarget = target.value / "plugins"
      IO.createDirectory(pluginsTarget)
      IO.copy(pluginFiles.map(file => file -> (pluginsTarget / file.getName)).toSet)
    },
    Test / fork           := true,
    // Assemble and stage the plugins before any test run, so the app can load them.
    Test / test           := (Test / test).dependsOn(copyPlugins).value,
    Test / testOnly       := (Test / testOnly).dependsOn(copyPlugins).evaluated,
    Test / testQuick      := (Test / testQuick).dependsOn(copyPlugins).evaluated,
    Universal / mappings ++= {
      // project-deletion ships disabled by default; every other plugin goes straight under plugins/.
      assembly.all(ScopeFilter(inProjects(pluginProjects *))).value.map { file =>
        val dir = if (file.getName.contains("project-deletion")) "plugins/disabled/" else "plugins/"
        file -> (dir + file.getName)
      }
    }
  )

lazy val testPlugin = project
  .in(file("delta/plugins/test-plugin"))
  .dependsOn(sdk % Provided, testkit % Provided)
  .settings(shared, compilation, noPublish)
  .settings(
    name                          := "delta-test-plugin",
    moduleName                    := "delta-test-plugin",
    assembly / assemblyOutputPath := target.value / "delta-test-plugin.jar",
    assembly / assemblyOption     := (assembly / assemblyOption).value.withIncludeScala(false),
    Test / fork                   := true
  )

lazy val blazegraphPlugin = project
  .in(file("delta/plugins/blazegraph"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings, pluginSettings("delta-blazegraph-plugin", "blazegraph.jar", "ai.senscience.nexus.delta.plugins.blazegraph"))
  .dependsOn(
    sdk % "provided;test->test"
  )

lazy val compositeViewsPlugin = project
  .in(file("delta/plugins/composite-views"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    pluginSettings("delta-composite-views-plugin", "composite-views.jar", "ai.senscience.nexus.delta.plugins.compositeviews"),
    libraryDependencies ++= http4sServerTest
  )
  .dependsOn(
    sdk              % "provided;test->test",
    elasticsearch    % "provided;test->test",
    blazegraphPlugin % "provided;test->test"
  )

lazy val searchPlugin = project
  .in(file("delta/plugins/search"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings, pluginSettings("delta-search-plugin", "search.jar", "ai.senscience.nexus.delta.plugins.search"))
  .dependsOn(
    sdk                  % "provided;test->test",
    elasticsearch        % "provided;test->compile;test->test",
    blazegraphPlugin     % "provided;test->compile;test->test",
    compositeViewsPlugin % "provided;test->compile;test->test"
  )

lazy val storagePlugin = project
  .in(file("delta/plugins/storage"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    pluginSettings("delta-storage-plugin", "storage.jar", "ai.senscience.nexus.delta.plugins.storage"),
    libraryDependencies ++= fs2Aws
  )
  .dependsOn(
    sdk           % "provided;test->test",
    elasticsearch % "provided;test->compile;test->test",
    testkit       % "test->compile"
  )

lazy val archivePlugin = project
  .in(file("delta/plugins/archive"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    pluginSettings("delta-archive-plugin", "archive.jar", "ai.senscience.nexus.delta.plugins.archive"),
    libraryDependencies += pekkoConnectorsFile
  )
  .dependsOn(
    sdk              % Provided,
    pekkoTestArchive % "test->compile",
    storagePlugin    % "provided;test->test"
  )

lazy val projectDeletionPlugin = project
  .in(file("delta/plugins/project-deletion"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    pluginSettings("delta-project-deletion-plugin", "project-deletion.jar", "ai.senscience.nexus.delta.plugins.projectdeletion")
  )
  .dependsOn(
    sdk % "provided;test->test"
  )

lazy val graphAnalyticsPlugin = project
  .in(file("delta/plugins/graph-analytics"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    pluginSettings("delta-graph-analytics-plugin", "graph-analytics.jar", "ai.senscience.nexus.delta.plugins.graph.analytics")
  )
  .dependsOn(
    sdk           % Provided,
    storagePlugin % "provided;test->test",
    elasticsearch % "provided;test->test"
  )

// Modules publishing JSON-LD contexts to the docs site
lazy val contextProjects: Seq[ProjectReference] = Seq(
  rdf,
  sdk,
  elasticsearch,
  archivePlugin,
  blazegraphPlugin,
  graphAnalyticsPlugin,
  compositeViewsPlugin,
  searchPlugin,
  storagePlugin
)

// The deployable plugins, assembled and shipped under the plugins/ directory of the distribution.
lazy val pluginProjects: Seq[ProjectReference] = Seq(
  blazegraphPlugin,
  compositeViewsPlugin,
  searchPlugin,
  storagePlugin,
  archivePlugin,
  projectDeletionPlugin,
  graphAnalyticsPlugin
)

lazy val plugins = project
  .in(file("delta/plugins"))
  .settings(shared, compilation, noPublish)
  .aggregate(pluginProjects *)
  .aggregate(testPlugin)

lazy val delta = project
  .in(file("delta"))
  .settings(shared, compilation, noPublish)
  .aggregate(kernel, testkit, sourcingPsql, rdf, sdk, elasticsearch, app, plugins)

lazy val tests = project
  .in(file("tests"))
  .dependsOn(pekkoMarshalling, testkit, pekkoTestArchive)
  .settings(noPublish)
  .settings(commonSettings)
  .settings(
    name                     := "tests",
    moduleName               := "tests",
    coverageFailOnMinimum    := false,
    libraryDependencies     ++= Seq(
      pekkoHttp,
      pekkoStream,
      circeOptics,
      circeGenericExtras,
      logback,
      pekkoTestKit       % Test,
      pekkoHttpTestKit   % Test,
      awsSdk             % Test,
      scalaTest          % Test,
      pekkoSlf4j         % Test,
      pekkoConnectorsSse % Test
    ) ++ fs2Aws,
    Test / parallelExecution := false,
    Test / testOptions       += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports"),
    Test / fork              := true
  )

lazy val benchmarks = project
  .in(file("benchmarks"))
  .dependsOn(kernel)
  .enablePlugins(GatlingPlugin)
  .settings(noPublish)
  .settings(shared, compilation)
  .settings(
    libraryDependencies   := gatling,
    Test / fork           := true,
    Gatling / javaOptions := overrideDefaultJavaOptions("-Xms1024m", "-Xmx4096m")
  )

lazy val root = project
  .in(file("."))
  .settings(name := "nexus", moduleName := "nexus")
  .settings(compilation, shared, noPublish)
  .aggregate(docs, pekko, delta, tests, benchmarks)

lazy val noPublish = Seq(
  publish / skip                         := true,
  Test / publish / skip                  := true,
  publishLocal                           := {},
  publish                                := {},
  publishArtifact                        := false,
  packageDoc / publishArtifact           := false,
  Compile / packageSrc / publishArtifact := false,
  Compile / packageDoc / publishArtifact := false,
  Test / packageBin / publishArtifact    := false,
  Test / packageDoc / publishArtifact    := false,
  Test / packageSrc / publishArtifact    := false,
  versionScheme                          := Some("strict")
)

lazy val assertJavaVersion =
  Seq(
    checkJavaVersion  := {
      val current  = VersionNumber(sys.props("java.specification.version"))
      val required = VersionNumber(javaSpecificationVersion.value)
      assert(CompatibleJavaVersion(current, required), s"Java '$required' or above required; current '$current'")
    },
    Compile / compile := (Compile / compile).dependsOn(checkJavaVersion).value
  )

lazy val shared = Seq(
  organization := "ai.senscience.nexus"
)

lazy val otelSettings = Seq(
  libraryDependencies ++= Seq(
    otelAutoconfigure,
    otelExporterOtlp
  ) ++ otelDependencies
)

lazy val discardModuleInfoAssemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case x if x.contains("io.netty.versions.properties") => MergeStrategy.discard
    case "module-info.class"                             => MergeStrategy.discard
    case x                                               =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)

lazy val compilation = {
  import sbt.Keys.*
  import sbt.*

  Seq(
    scalaVersion                           := scalaCompilerVersion,
    scalacOptions                          ~= { options: Seq[String] =>
      options.filterNot(Set("-Wself-implicit", "-Xlint:infer-any", "-Xfatal-warnings", "-Wnonunit-statement")) ++
        Seq("-source:future", "-Yretain-trees", "-no-indent", "-Wunused:all", "-Werror")
    },
    javaSpecificationVersion               := "25",
    javacOptions                          ++= Seq(
      "-source",
      javaSpecificationVersion.value,
      "-target",
      javaSpecificationVersion.value,
      "-Xlint"
    ),
    excludeDependencies                   ++= Seq(
      ExclusionRule("log4j", "log4j"),
      ExclusionRule("org.apache.logging.log4j", "log4j-api"),
      ExclusionRule("org.apache.logging.log4j", "log4j-core")
    ),
    Compile / packageSrc / publishArtifact := !isSnapshot.value,
    Compile / packageDoc / publishArtifact := !isSnapshot.value,
    Compile / doc / scalacOptions         ++= Seq("-no-link-warnings"),
    Compile / doc / javacOptions           := Seq("-source", javaSpecificationVersion.value),
    // Workaround for scaladoc error during publishing
    Compile / packageDoc / publishArtifact := false,
    // scalac-compat-annotation is needed on the classpath for scaladoc to resolve @nowarn213 in fs2 TASTy files
    libraryDependencies                    += scalacCompat % Provided,
    autoAPIMappings                        := true,
    apiMappings                            += {
      val scalaDocUrl = s"http://scala-lang.org/api/${scalaVersion.value}/"
      ApiMappings.apiMappingFor((Compile / fullClasspath).value)("scala3-library", scalaDocUrl)
    }
  )
}

lazy val coverage = Seq(
  coverageMinimumStmtTotal := 65,
  coverageFailOnMinimum    := true
)

lazy val release = Seq(
  Test / publish / skip               := true,
  Test / packageBin / publishArtifact := false,
  Test / packageDoc / publishArtifact := false,
  Test / packageSrc / publishArtifact := false,
  // removes compile time only dependencies from the resulting pom
  pomPostProcess                      := { node =>
    XmlTransformer.transformer(moduleFilter("org.scoverage")).transform(node).head
  },
  versionScheme                       := Some("strict")
)

// Settings shared by every published delta module.
lazy val commonSettings = shared ++ compilation ++ assertJavaVersion ++ coverage ++ release

// Settings shared by every assembled plugin. `moduleId` is the artifact/module name (e.g. delta-blazegraph-plugin),
// `jarName` the assembled jar (e.g. blazegraph.jar) and `infoPackage` the target package for BuildInfo.
def pluginSettings(moduleId: String, jarName: String, infoPackage: String) = {
  val pluginArtifact = Artifact(moduleId, "plugin")
  Seq(
    name                       := moduleId,
    moduleName                 := moduleId,
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := infoPackage,
    assembly / assemblyJarName := jarName,
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    Test / fork                := true,
    artifacts                  += pluginArtifact,
    packagedArtifacts          := packagedArtifacts.value.updated(pluginArtifact, assembly.value)
  ) ++ discardModuleInfoAssemblySettings
}

lazy val servicePackaging = {
  import com.typesafe.sbt.packager.Keys._
  import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.{Docker, dockerChmodType}
  import com.typesafe.sbt.packager.docker.{DockerChmodType, DockerVersion}
  import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport.Universal
  Seq(
    // Docker publishing settings
    Docker / maintainer   := "Nexus Team <noreply@senscience.ai>",
    Docker / version      := {
      if (isSnapshot.value) "latest"
      else version.value
    },
    Docker / daemonUser   := "nexus",
    dockerBaseImage       := "eclipse-temurin:25-jre",
    dockerBuildxPlatforms := Seq("linux/arm64/v8", "linux/amd64"),
    dockerExposedPorts    := Seq(8080),
    dockerRepository      := Some("ghcr.io"),
    dockerUsername        := Some("senscience"),
    dockerUpdateLatest    := false,
    dockerChmodType       := DockerChmodType.UserGroupWriteExecute
  )
}

// Scalafix
ThisBuild / semanticdbEnabled    := true
ThisBuild / semanticdbVersion    := scalafixSemanticdb.revision
ThisBuild / scalafixDependencies += "org.typelevel" %% "typelevel-scalafix" % typelevelScalafixVersion

ThisBuild / homepage   := Some(url("https://senscience.github.io/nexus-delta/docs/"))
ThisBuild / licenses   := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / scmInfo    := Some(ScmInfo(url("https://github.com/senscience/nexus-delta"), "scm:git:git@github.com:senscience/nexus-delta.git"))
ThisBuild / developers := List(
  Developer("imsdu", "Simon Dumas", "noreply@senscience.ai", url("https://www.senscience.ai/"))
)

Global / excludeLintKeys        += packageDoc / publishArtifact
Global / excludeLintKeys        += docs / paradoxRoots
Global / excludeLintKeys        += docs / Paradox / paradoxNavigationDepth
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

// Documentation
addCommandAlias("build-docs", ";docs/clean;docs/makeSite")
addCommandAlias("preview-docs", ";docs/clean;docs/previewSite")

// Static analysis
val staticAnalysis = List(
  "scalafmtCheckAll",
  "scalafixAll --check",
  "doc"
).mkString(";")
addCommandAlias("static-analysis", staticAnalysis)

// Testing
val coreModules = List("kernel", "pekkoMarshalling", "pekkoTestArchive", "rdf", "sdk", "sourcingPsql", "elasticsearch", "testkit")

def runTests(modules: String*) =
  modules.map(module => s"$module/test").mkString(";")

def runTestsWithCoverage(modules: String*) =
  "coverage;" +
    runTests(modules *) +
    modules.map(module => s"$module/coverageReport").mkString(";", ";", "")

addCommandAlias("core-unit-tests", runTests(coreModules *))
addCommandAlias("core-unit-tests-with-coverage", runTestsWithCoverage(coreModules *))
addCommandAlias("app-unit-tests", runTests("app"))
addCommandAlias("app-unit-tests-with-coverage", runTestsWithCoverage("app"))
addCommandAlias("plugins-unit-tests", runTests("plugins"))
addCommandAlias("plugins-unit-tests-with-coverage", runTestsWithCoverage("plugins"))
addCommandAlias("integration-tests", runTests("tests"))
