package ai.senscience.nexus.testkit.errors.files

import ai.senscience.nexus.testkit.CirceLiteral.circeLiteralSyntax

object FileErrors {

  def fileIsNotDeprecatedError(id: String) =
    json"""
      {
        "@context" : "https://bluebrain.github.io/nexus/contexts/error.json",
        "@type" : "FileIsNotDeprecated",
        "reason" : "File '$id' is not deprecated."
      }
    """

  def fileAlreadyExistsError(id: String) =
    json"""
      {
        "@context": "https://bluebrain.github.io/nexus/contexts/error.json",
        "@type": "ResourceAlreadyExists",
        "reason": "Resource '$id' already exists in project 'org/proj'."
      }
    """

}
