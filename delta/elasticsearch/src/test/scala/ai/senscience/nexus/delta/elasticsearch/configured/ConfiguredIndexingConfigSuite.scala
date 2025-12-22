package ai.senscience.nexus.delta.elasticsearch.configured

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexingConfig.ConfiguredIndex
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.error.LoadFileError.UnaccessibleFile
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.testkit.file.TempDirectory
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import com.typesafe.config.ConfigFactory
import fs2.io.file.Path
import munit.AnyFixture

class ConfiguredIndexingConfigSuite extends NexusSuite with TempDirectory.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(tempDirectory)

  private lazy val tempDir = tempDirectory()

  test("Load nothing if it is disabled") {
    val config = ConfigFactory.parseString(
      """
        |app.elasticsearch.configured-indexing {
        | enabled = false
        |}
        |""".stripMargin
    )
    ConfiguredIndexingConfig.load(config).assertEquals(ConfiguredIndexingConfig.Disabled)
  }

  test("Load the config and the external files if enabled") {

    val firstType  = nxv + "First"
    val secondType = nxv + "Second"

    val firstMappingContent  = jobj"""{ "mapping1":  ""}"""
    val firstSettingsContent = jobj"""{ "settings1":  ""}"""
    val secondMappingContent = jobj"""{ "mapping2":  ""}"""

    def parseConfig(firstMapping: Path, firstSettings: Path, secondMapping: Path) =
      ConfigFactory.parseString(
        s"""
          |app.elasticsearch.configured-indexing {
          | enabled = true
          | prefix = test
          | values = [
          |   {
          |     index = "index1"
          |     mapping = ${firstMapping.absolute}
          |     settings = ${firstSettings.absolute}
          |     types = ["$firstType"]
          |   },
          |   {
          |     index = "index2"
          |     mapping = ${secondMapping.absolute}
          |     types = ["$secondType"]
          |   }
          | ]
          |}
          |""".stripMargin
      )

    for {
      firstMapping  <- TempDirectory.writeJson(tempDir, genString(), firstMappingContent)
      firstSettings <- TempDirectory.writeJson(tempDir, genString(), firstSettingsContent)
      secondMapping <- TempDirectory.writeJson(tempDir, genString(), secondMappingContent)
      config         = parseConfig(firstMapping, firstSettings, secondMapping)
      expected       = ConfiguredIndexingConfig.Enabled(
                         "test",
                         NonEmptyList.of(
                           ConfiguredIndex(
                             IndexLabel.unsafe("index1"),
                             ElasticsearchIndexDef.fromJson(firstMappingContent, Some(firstSettingsContent)),
                             Set(firstType)
                           ),
                           ConfiguredIndex(
                             IndexLabel.unsafe("index2"),
                             ElasticsearchIndexDef.fromJson(secondMappingContent, None),
                             Set(secondType)
                           )
                         )
                       )
      _             <- ConfiguredIndexingConfig.load(config).assertEquals(expected)
    } yield ()
  }

  test("Fail to load the config and if the index files do not exist") {
    val config = ConfigFactory.parseString(
      """
         |app.elasticsearch.configured-indexing {
         | enabled = true
         | prefix = test
         | values = [
         |   {
         |     index = "index1"
         |     mapping = /do/not/exist
         |     settings = /do/not/exist
         |     types = []
         |   },
         | ]
         |}
         |""".stripMargin
    )
    ConfiguredIndexingConfig.load(config).intercept[UnaccessibleFile]
  }
}
