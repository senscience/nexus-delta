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

val scalacScapegoatVersion = "3.1.9"
val scalaCompilerVersion   = "3.3.6"

val awsSdkVersion              = "2.34.7"
val caffeineVersion            = "3.2.2"
val catsEffectVersion          = "3.6.3"
val catsRetryVersion           = "4.0.0"
val catsVersion                = "2.13.0"
val circeVersion               = "0.14.15"
val circeOpticsVersion         = "0.15.1"
val circeExtrasVersions        = "0.14.5-RC1"
val classgraphVersion          = "4.8.181"
val distageVersion             = "1.2.20"
val doobieVersion              = "1.0.0-RC10"
val fs2Version                 = "3.12.2"
val fs2AwsVersion              = "6.3.0"
val gatlingVersion             = "3.14.5"
val glassFishJakartaVersion    = "2.0.1"
val handleBarsVersion          = "4.5.0"
val hikariVersion              = "7.0.2"
val http4sVersion              = "0.23.32"
val http4sXMLVersion           = "0.24.0"
val jenaVersion                = "5.2.0"
val jsonIterVersion            = "2.38.2"
val log4catsVersion            = "2.7.1"
val logbackVersion             = "1.5.19"
val magnoliaVersion            = "1.3.18"
val munitVersion               = "1.2.0"
val munitCatsEffectVersion     = "2.1.0"
val nimbusJoseJwtVersion       = "10.5"
val otelVersion                = "1.54.1"
val otel4sVersion              = "0.13.2"
val otelLogbackVersion         = "2.20.1-alpha"
val pekkoVersion               = "1.1.5"
val pekkoConnectorsVersion     = "1.2.0"
val pekkoHttpVersion           = "1.2.0"
val postgresJdbcVersion        = "42.7.8"
val pureconfigVersion          = "0.17.9"
val scalaTestVersion           = "3.2.19"
val scalaXmlVersion            = "2.4.0"
val shapeless3Version          = "3.5.0"
val titaniumJsonLdVersion      = "1.6.0"
val topBraidVersion            = "1.4.4"
val testContainersVersion      = "1.21.3"
val testContainersScalaVersion = "0.43.0"

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
lazy val doobiePostgres     = "org.tpolecat"                 %% "doobie-postgres"      % doobieVersion
lazy val doobie             = Seq(
  doobiePostgres,
  "org.tpolecat"  %% "doobie-hikari" % doobieVersion,
  "com.zaxxer"     % "HikariCP"      % hikariVersion exclude ("org.slf4j", "slf4j-api"),
  "org.postgresql" % "postgresql"    % postgresJdbcVersion
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

lazy val jenaArq = "org.apache.jena" % "jena-arq" % jenaVersion

lazy val jsoniterCirce = "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-circe" % jsonIterVersion

lazy val log4cats        = "org.typelevel"                %% "log4cats-slf4j"    % log4catsVersion
lazy val logback         = "ch.qos.logback"                % "logback-classic"   % logbackVersion
lazy val magnolia        = "com.softwaremill.magnolia1_3" %% "magnolia"          % magnoliaVersion
lazy val munit           = "org.scalameta"                %% "munit"             % munitVersion
lazy val munitCatsEffect = "org.typelevel"                %% "munit-cats-effect" % munitCatsEffectVersion
lazy val nimbusJoseJwt   = "com.nimbusds"                  % "nimbus-jose-jwt"   % nimbusJoseJwtVersion

lazy val otel4s            = "org.typelevel"                   %% "otel4s-oteljava"                           % otel4sVersion
lazy val otel4sStorage     = "org.typelevel"                   %% "otel4s-oteljava-context-storage"           % otel4sVersion
lazy val otel4sSemconv     = "org.typelevel"                   %% "otel4s-semconv"                            % otel4sVersion
lazy val otelAutoconfigure = "io.opentelemetry"                 % "opentelemetry-sdk-extension-autoconfigure" % otelVersion        % Runtime
lazy val otelExporterOtlp  = "io.opentelemetry"                 % "opentelemetry-exporter-otlp"               % otelVersion        % Runtime
lazy val otelLogback       = "io.opentelemetry.instrumentation" % "opentelemetry-logback-appender-1.0"        % otelLogbackVersion
lazy val otelLogbackMdc    = "io.opentelemetry.instrumentation" % "opentelemetry-logback-mdc-1.0"             % otelLogbackVersion

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
lazy val topBraidShacl                 = "org.topbraid"            % "shacl"                              % topBraidVersion
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
  .disablePlugins(ScapegoatSbtPlugin)
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
      val contextDirs                        = Seq(
        (rdf / Compile / resourceDirectory).value / "contexts",
        (sdk / Compile / resourceDirectory).value / "contexts",
        (elasticsearch / Compile / resourceDirectory).value / "contexts",
        (archivePlugin / Compile / resourceDirectory).value / "contexts",
        (blazegraphPlugin / Compile / resourceDirectory).value / "contexts",
        (graphAnalyticsPlugin / Compile / resourceDirectory).value / "contexts",
        (compositeViewsPlugin / Compile / resourceDirectory).value / "contexts",
        (searchPlugin / Compile / resourceDirectory).value / "contexts",
        (storagePlugin / Compile / resourceDirectory).value / "contexts"
      )
      contextDirs.flatMap { dir =>
        fileSources(dir).map(file => file -> s"contexts/${file.getName}")
      }
    }
  )

lazy val pekkoMarshalling = project
  .in(file("pekko/marshalling"))
  .settings(name := "pekko-marshalling", moduleName := "pekko-marshalling")
  .settings(shared, compilation, coverage, release, assertJavaVersion)
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
  .settings(shared, compilation, coverage, release, assertJavaVersion)
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
  .settings(shared, compilation, coverage, release, assertJavaVersion)
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
      munit           % Test,
      munitCatsEffect % Test
    ) ++ pureConfig ++ http4s
  )

lazy val testkit = project
  .dependsOn(kernel)
  .in(file("delta/testkit"))
  .settings(name := "delta-testkit", moduleName := "delta-testkit")
  .settings(shared, compilation, coverage, release, assertJavaVersion)
  .settings(
    coverageMinimumStmtTotal := 0,
    libraryDependencies     ++= Seq(
      logback,
      otel4s,
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
  .settings(shared, compilation, assertJavaVersion, coverage, release)
  .settings(
    libraryDependencies  ++= Seq(
      classgraph,
      distageCore,
      shapeless3Typeable
    ) ++ doobie,
    coverageFailOnMinimum := false,
    Test / fork           := true
  )

lazy val rdf = project
  .in(file("delta/rdf"))
  .dependsOn(kernel, testkit % "test->compile")
  .settings(shared, compilation, assertJavaVersion, coverage, release)
  .settings(
    name       := "delta-rdf",
    moduleName := "delta-rdf"
  )
  .settings(
    libraryDependencies ++= Seq(
      catsCore,
      glassFishJakarta,
      jenaArq,
      magnolia,
      titaniumJsonLd,
      topBraidShacl
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
  .settings(shared, compilation, assertJavaVersion, coverage, release)
  .settings(
    coverageFailOnMinimum := false,
    libraryDependencies  ++= Seq(
      pekkoHttpXml exclude ("org.scala-lang.modules", "scala-xml_3"),
      scalaXml,
      distageCore,
      otel4s,
      otel4sSemconv,
      pekkoSlf4j       % Test,
      pekkoTestKit     % Test,
      pekkoHttpTestKit % Test
    )
  )

lazy val elasticsearch = project
  .in(file("delta/elasticsearch"))
  .settings(shared, compilation, assertJavaVersion, coverage, release)
  .dependsOn(
    sdk % "compile->compile;test->test"
  )
  .settings(
    coverageFailOnMinimum := false,
    Test / fork           := true
  )

lazy val app = project
  .in(file("delta/app"))
  .settings(
    name       := "delta-app",
    moduleName := "delta-app"
  )
  .enablePlugins(UniversalPlugin, JavaAppPackaging, DockerPlugin, BuildInfoPlugin)
  .settings(shared, compilation, servicePackaging, assertJavaVersion, otelSettings, coverage, release)
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
    buildInfoKeys         := Seq[BuildInfoKey](version),
    buildInfoPackage      := "ai.senscience.nexus.delta.config",
    Docker / packageName  := "nexus-delta",
    copyPlugins           := {
      val bgFile              = (blazegraphPlugin / assembly).value
      val graphAnalyticsFile  = (graphAnalyticsPlugin / assembly).value
      val storageFile         = (storagePlugin / assembly).value
      val archiveFile         = (archivePlugin / assembly).value
      val compositeViewsFile  = (compositeViewsPlugin / assembly).value
      val searchFile          = (searchPlugin / assembly).value
      val projectDeletionFile = (projectDeletionPlugin / assembly).value
      val pluginsTarget       = target.value / "plugins"
      IO.createDirectory(pluginsTarget)
      IO.copy(
        Set(
          bgFile              -> (pluginsTarget / bgFile.getName),
          graphAnalyticsFile  -> (pluginsTarget / graphAnalyticsFile.getName),
          storageFile         -> (pluginsTarget / storageFile.getName),
          archiveFile         -> (pluginsTarget / archiveFile.getName),
          compositeViewsFile  -> (pluginsTarget / compositeViewsFile.getName),
          searchFile          -> (pluginsTarget / searchFile.getName),
          projectDeletionFile -> (pluginsTarget / projectDeletionFile.getName)
        )
      )
    },
    Test / fork           := true,
    Test / test           := {
      val _ = copyPlugins.value
      (Test / test).value
    },
    Test / testOnly       := {
      val _ = copyPlugins.value
      (Test / testOnly).evaluated
    },
    Test / testQuick      := {
      val _ = copyPlugins.value
      (Test / testQuick).evaluated
    },
    Universal / mappings ++= {
      val bgFile              = (blazegraphPlugin / assembly).value
      val graphAnalytics      = (graphAnalyticsPlugin / assembly).value
      val storageFile         = (storagePlugin / assembly).value
      val archiveFile         = (archivePlugin / assembly).value
      val compositeViewsFile  = (compositeViewsPlugin / assembly).value
      val searchFile          = (searchPlugin / assembly).value
      val projectDeletionFile = (projectDeletionPlugin / assembly).value
      Seq(
        (bgFile, "plugins/" + bgFile.getName),
        (graphAnalytics, "plugins/" + graphAnalytics.getName),
        (storageFile, "plugins/" + storageFile.getName),
        (archiveFile, "plugins/" + archiveFile.getName),
        (compositeViewsFile, "plugins/" + compositeViewsFile.getName),
        (searchFile, "plugins/" + searchFile.getName),
        (projectDeletionFile, "plugins/disabled/" + projectDeletionFile.getName)
      )
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
  .settings(shared, compilation, assertJavaVersion, discardModuleInfoAssemblySettings, coverage, release)
  .dependsOn(
    sdk % "provided;test->test"
  )
  .settings(
    name                       := "delta-blazegraph-plugin",
    moduleName                 := "delta-blazegraph-plugin",
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := "ai.senscience.nexus.delta.plugins.blazegraph",
    coverageFailOnMinimum      := false,
    assembly / assemblyJarName := "blazegraph.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    addArtifact(Artifact("delta-blazegraph-plugin", "plugin"), assembly),
    Test / fork                := true
  )

lazy val compositeViewsPlugin = project
  .in(file("delta/plugins/composite-views"))
  .enablePlugins(BuildInfoPlugin)
  .settings(shared, compilation, assertJavaVersion, discardModuleInfoAssemblySettings, coverage, release)
  .dependsOn(
    sdk              % "provided;test->test",
    elasticsearch    % "provided;test->test",
    blazegraphPlugin % "provided;test->test"
  )
  .settings(
    name                       := "delta-composite-views-plugin",
    moduleName                 := "delta-composite-views-plugin",
    libraryDependencies       ++= http4sServerTest,
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := "ai.senscience.nexus.delta.plugins.compositeviews",
    coverageFailOnMinimum      := false, // TODO: Remove this line when coverage increases
    assembly / assemblyJarName := "composite-views.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    addArtifact(Artifact("delta-composite-views-plugin", "plugin"), assembly),
    Test / fork                := true
  )

lazy val searchPlugin = project
  .in(file("delta/plugins/search"))
  .enablePlugins(BuildInfoPlugin)
  .settings(shared, compilation, assertJavaVersion, discardModuleInfoAssemblySettings, coverage, release)
  .dependsOn(
    sdk                  % "provided;test->test",
    elasticsearch        % "provided;test->compile;test->test",
    blazegraphPlugin     % "provided;test->compile;test->test",
    compositeViewsPlugin % "provided;test->compile;test->test"
  )
  .settings(
    name                       := "delta-search-plugin",
    moduleName                 := "delta-search-plugin",
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := "ai.senscience.nexus.delta.plugins.search",
    coverageFailOnMinimum      := false, // TODO: Remove this line when coverage increases
    assembly / assemblyJarName := "search.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    addArtifact(Artifact("delta-search-plugin", "plugin"), assembly),
    Test / fork                := true
  )

lazy val storagePlugin = project
  .enablePlugins(BuildInfoPlugin)
  .in(file("delta/plugins/storage"))
  .settings(shared, compilation, assertJavaVersion, discardModuleInfoAssemblySettings, coverage, release)
  .dependsOn(
    sdk           % "provided;test->test",
    elasticsearch % "provided;test->compile;test->test",
    testkit       % "test->compile"
  )
  .settings(
    name                       := "delta-storage-plugin",
    moduleName                 := "delta-storage-plugin",
    libraryDependencies       ++= fs2Aws,
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := "ai.senscience.nexus.delta.plugins.storage",
    coverageFailOnMinimum      := false, // TODO: Remove this line when coverage increases
    assembly / assemblyJarName := "storage.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    addArtifact(Artifact("delta-storage-plugin", "plugin"), assembly),
    Test / fork                := true
  )

lazy val archivePlugin = project
  .in(file("delta/plugins/archive"))
  .enablePlugins(BuildInfoPlugin)
  .settings(shared, compilation, assertJavaVersion, discardModuleInfoAssemblySettings, coverage, release)
  .dependsOn(
    sdk              % Provided,
    pekkoTestArchive % "test->compile",
    storagePlugin    % "provided;test->test"
  )
  .settings(
    name                       := "delta-archive-plugin",
    moduleName                 := "delta-archive-plugin",
    libraryDependencies        += pekkoConnectorsFile,
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := "ai.senscience.nexus.delta.plugins.archive",
    coverageFailOnMinimum      := false,
    assembly / assemblyJarName := "archive.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    addArtifact(Artifact("delta-archive-plugin", "plugin"), assembly),
    Test / fork                := true
  )

lazy val projectDeletionPlugin = project
  .in(file("delta/plugins/project-deletion"))
  .enablePlugins(BuildInfoPlugin)
  .settings(shared, compilation, assertJavaVersion, discardModuleInfoAssemblySettings, coverage, release)
  .dependsOn(
    sdk % "provided;test->test"
  )
  .settings(
    name                       := "delta-project-deletion-plugin",
    moduleName                 := "delta-project-deletion-plugin",
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := "ai.senscience.nexus.delta.plugins.projectdeletion",
    assembly / assemblyJarName := "project-deletion.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    addArtifact(Artifact("delta-project-deletion-plugin", "plugin"), assembly),
    Test / fork                := true,
    coverageFailOnMinimum      := false
  )

lazy val graphAnalyticsPlugin = project
  .in(file("delta/plugins/graph-analytics"))
  .enablePlugins(BuildInfoPlugin)
  .settings(shared, compilation, assertJavaVersion, discardModuleInfoAssemblySettings, coverage, release)
  .dependsOn(
    sdk           % Provided,
    storagePlugin % "provided;test->test",
    elasticsearch % "provided;test->test"
  )
  .settings(
    name                       := "delta-graph-analytics-plugin",
    moduleName                 := "delta-graph-analytics-plugin",
    buildInfoKeys              := Seq[BuildInfoKey](version),
    buildInfoPackage           := "ai.senscience.nexus.delta.plugins.graph.analytics",
    assembly / assemblyJarName := "graph-analytics.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / test            := {},
    addArtifact(Artifact("delta-graph-analytics-plugin", "plugin"), assembly),
    Test / fork                := true,
    coverageFailOnMinimum      := false
  )

lazy val plugins = project
  .in(file("delta/plugins"))
  .settings(shared, compilation, noPublish)
  .aggregate(
    blazegraphPlugin,
    compositeViewsPlugin,
    searchPlugin,
    storagePlugin,
    archivePlugin,
    projectDeletionPlugin,
    testPlugin,
    graphAnalyticsPlugin
  )

lazy val delta = project
  .in(file("delta"))
  .settings(shared, compilation, noPublish)
  .aggregate(kernel, testkit, sourcingPsql, rdf, sdk, elasticsearch, app, plugins)

lazy val tests = project
  .in(file("tests"))
  .dependsOn(pekkoMarshalling, testkit, pekkoTestArchive)
  .settings(noPublish)
  .settings(shared, compilation, coverage, release)
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
  .disablePlugins(ScapegoatSbtPlugin)
  .settings(noPublish)
  .settings(shared, compilation)
  .settings(
    libraryDependencies := gatling,
    Test / fork         := true
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
    otelExporterOtlp,
    otelLogback,
    otelLogbackMdc
  ),
  // Enable Cats Effect fiber context tracking
  javaOptions          += "-Dcats.effect.trackFiberContext=true"
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
      options.filterNot(Set("-Wself-implicit", "-Xlint:infer-any", "-Wnonunit-statement")) ++
        Seq("-source:future", "-Yretain-trees", "-no-indent", "-Wunused:all")
    },
    javaSpecificationVersion               := "21",
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
    autoAPIMappings                        := true,
    apiMappings                            += {
      val scalaDocUrl = s"http://scala-lang.org/api/${scalaVersion.value}/"
      ApiMappings.apiMappingFor((Compile / fullClasspath).value)("scala3-library", scalaDocUrl)
    },
    Scapegoat / dependencyClasspath        := (Compile / dependencyClasspath).value
  )
}

lazy val coverage = Seq(
  coverageMinimumStmtTotal := 75,
  coverageFailOnMinimum    := true
)

lazy val release = Seq(
  Test / publish / skip               := true,
  Test / packageBin / publishArtifact := false,
  Test / packageDoc / publishArtifact := false,
  Test / packageSrc / publishArtifact := false,
  // removes compile time only dependencies from the resulting pom
  pomPostProcess                      := { node =>
    XmlTransformer.transformer(moduleFilter("org.scoverage") | moduleFilter("com.sksamuel.scapegoat")).transform(node).head
  },
  versionScheme                       := Some("strict")
)

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
    dockerBaseImage       := "eclipse-temurin:21-jre",
    dockerBuildxPlatforms := Seq("linux/arm64/v8", "linux/amd64"),
    dockerExposedPorts    := Seq(8080),
    dockerRepository      := Some("ghcr.io"),
    dockerUsername        := Some("senscience"),
    dockerUpdateLatest    := false,
    dockerChmodType       := DockerChmodType.UserGroupWriteExecute
  )
}

ThisBuild / scapegoatVersion             := scalacScapegoatVersion
ThisBuild / scapegoatDisabledInspections := Seq(
  "AsInstanceOf",
  "ClassNames",
  "IncorrectlyNamedExceptions",
  "ObjectNames",
  "RedundantFinalModifierOnCaseClass",
  "RedundantFinalModifierOnMethod",
  "VariableShadowing"
)
ThisBuild / homepage                     := Some(url("https://senscience.github.io/nexus-delta/docs/"))
ThisBuild / licenses                     := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / scmInfo                      := Some(ScmInfo(url("https://github.com/senscience/nexus-delta"), "scm:git:git@github.com:senscience/nexus-delta.git"))
ThisBuild / developers                   := List(
  Developer("imsdu", "Simon Dumas", "noreply@senscience.ai", url("https://www.senscience.ai/"))
)

Global / excludeLintKeys        += packageDoc / publishArtifact
Global / excludeLintKeys        += docs / paradoxRoots
Global / excludeLintKeys        += docs / Paradox / paradoxNavigationDepth
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

addCommandAlias(
  "review",
  s"""
     |;clean
     |;scalafmtCheck
     |;test:scalafmtCheck
     |;scalafmtSbtCheck
     |;coverage
     |;scapegoat
     |;test
     |;coverageReport
     |;coverageAggregate
     |""".stripMargin
)
addCommandAlias(
  "deltaReview",
  """
     |;delta/clean
     |;delta/scalafmtCheck
     |;delta/test:scalafmtCheck
     |;scalafmtSbtCheck;coverage
     |;delta/scapegoat
     |;delta/test
     |;delta/coverageReport
     |;delta/coverageAggregate
     |""".stripMargin
)
addCommandAlias("build-docs", ";docs/clean;docs/makeSite")
addCommandAlias("preview-docs", ";docs/clean;docs/previewSite")

val coreModules = List("kernel", "pekkoMarshalling", "pekkoTestArchive", "rdf", "sdk", "sourcingPsql", "elasticsearch", "testkit")

val staticAnalysis =
  s"""
    |scalafmtSbtCheck ;
    |scalafmtCheck ;
    |Test/scalafmtCheck ;
    |scapegoat ;
    |doc
    |""".stripMargin

addCommandAlias("static-analysis", staticAnalysis)

def runTestsWithCoverageCommandsForModules(modules: List[String]) = {
  ";coverage" +
    modules.map(module => s";$module/test").mkString +
    modules.map(module => s";$module/coverageReport").mkString
}
def runTestsCommandsForModules(modules: List[String])             = {
  modules.map(module => s";$module/test").mkString
}

addCommandAlias("core-unit-tests", runTestsCommandsForModules(coreModules))
addCommandAlias("core-unit-tests-with-coverage", runTestsWithCoverageCommandsForModules(coreModules))
addCommandAlias("app-unit-tests", runTestsCommandsForModules(List("app")))
addCommandAlias("app-unit-tests-with-coverage", runTestsWithCoverageCommandsForModules(List("app")))
addCommandAlias("plugins-unit-tests", runTestsCommandsForModules(List("plugins")))
addCommandAlias("plugins-unit-tests-with-coverage", runTestsWithCoverageCommandsForModules(List("plugins")))
addCommandAlias("integration-tests", runTestsCommandsForModules(List("tests")))
