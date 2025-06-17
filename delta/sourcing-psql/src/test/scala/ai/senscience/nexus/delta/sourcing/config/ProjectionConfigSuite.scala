package ai.senscience.nexus.delta.sourcing.config

import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig.ClusterConfig
import ai.senscience.nexus.testkit.mu.NexusSuite
import pureconfig.ConfigSource

class ProjectionConfigSuite extends NexusSuite {

  private def parseConfig(nodeIndex: Int, size: Int) =
    ConfigSource
      .string(s"""
         |cluster {
         |  node-index = $nodeIndex
         |  size = $size
         |}
         |""".stripMargin)
      .at("cluster")
      .load[ClusterConfig]

  test("Parse successfully when the node index is lower than the cluster size") {
    parseConfig(1, 2).assertRight(ClusterConfig(2, 1))
  }

  test("Fail to parse when the node index is higher than the cluster size") {
    parseConfig(2, 1).assertLeft()
  }

}
