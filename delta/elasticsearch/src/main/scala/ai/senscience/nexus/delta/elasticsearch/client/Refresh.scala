package ai.senscience.nexus.delta.elasticsearch.client

enum Refresh(val value: String) {
  case True    extends Refresh("true")
  case False   extends Refresh("true")
  case WaitFor extends Refresh("wait_for")
}
