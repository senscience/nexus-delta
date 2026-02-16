package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel.{IllegalIndexLabel, IndexGroup}
import ai.senscience.nexus.testkit.mu.NexusSuite

class IndexLabelSuite extends NexusSuite {

  private val invalidLabels = List(".", ".s", "+s", "s*e", "s?e", "s/e", "s|e", "s\\e", "s,e", genString(length = 210))

  invalidLabels.foreach { index =>
    test(s"IndexLabel fails for invalid input '$index'") {
      IndexLabel(index).assertLeft(IllegalIndexLabel(index))
    }

    test(s"IndexGroup fails for invalid input '$index'") {
      IndexGroup(index).assertLeft(IllegalIndexLabel(index))
    }
  }

  test("IndexGroup fails for a non-prefixed label") {
    val index = genString()
    IndexGroup(index).assertLeft(IllegalIndexLabel(index))
  }

  test("IndexLabel succeeds for a valid label") {
    val index = genString()
    IndexLabel(index).map(_.value).assertRight(index)
  }

  test("IndexGroup succeeds for a valid group") {
    val index = genString(10)
    IndexGroup(index).map(_.value).assertRight(index)
  }
}
