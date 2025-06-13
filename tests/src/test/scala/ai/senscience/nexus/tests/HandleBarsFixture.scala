package ai.senscience.nexus.tests

import ai.senscience.nexus.tests.config.TestsConfig
import Identity.Authenticated

/**
  * Utility methods for the handlebars templating
  */
trait HandleBarsFixture {

  def replacements(authenticated: Authenticated, otherReplacements: (String, String)*)(implicit
      config: TestsConfig
  ): Seq[(String, String)] =
    Seq(
      "deltaUri" -> config.deltaUri.toString(),
      "realm"    -> authenticated.realm.name,
      "user"     -> authenticated.name
    ) ++ otherReplacements

}
