import sbt.*
import xsbti.{FileConverter, HashedVirtualFileRef}

object ApiMappings {

  /**
    * <p>Constructs a tuple (xsbti.HashedVirtualFileRef -> java.net.URI) by searching the ''classpath'' for the jar file
    * with the argument ''artifactId''. The value returned can be appended to the ''apiMappings'' such that
    * documentation linking works when building the docs.</p>
    *
    * <p>For example:
    * {{{
    *   lazy val myProject = project.settings(Seq(
    *     apiMappings += {
    *       val scalaDocUrl = "http://scala-lang.org/api/" + scalaVersion.value + "/"
    *       apiMappingFor((Compile / fullClasspath).value, fileConverter.value)("scala3-library", scalaDocUrl)
    *     }
    *   ))
    * }}}
    * </p>
    *
    * @param classpath
    *   the classpath to search
    * @param converter
    *   converts the classpath entries to files
    * @param artifactId
    *   the artifact that provides the linked type
    * @param address
    *   the full URL (as string) of the artifact documentation
    * @return
    *   a tuple (xsbti.HashedVirtualFileRef -> java.net.URI) to be appended to the ''apiMappings''
    */
  final def apiMappingFor(classpath: Seq[Attributed[HashedVirtualFileRef]], converter: FileConverter)(
      artifactId: String,
      address: String
  ): (HashedVirtualFileRef, URI) = {
    @SuppressWarnings(Array("OptionGet"))
    def findJar(nameBeginsWith: String): HashedVirtualFileRef = {
      classpath
        .find { attributed => (converter.toPath(attributed.data).toFile ** s"$nameBeginsWith*.jar").get().nonEmpty }
        .get
        .data // fail hard if not found
    }
    findJar(artifactId) -> uri(address)
  }
}
