package ai.senscience.nexus.delta.plugins.storage.files

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriPath
import ai.senscience.nexus.delta.plugins.storage.files.generators.FileGen
import ai.senscience.nexus.delta.plugins.storage.files.model.{FileAttributes, FileCustomMetadata}
import ai.senscience.nexus.delta.plugins.storage.storages.model.AbsolutePath
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.Generators
import akka.http.scaladsl.model.ContentTypes.`text/plain(UTF-8)`
import akka.http.scaladsl.model.{HttpEntity, MessageEntity, Multipart}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}

import java.util.{Base64, UUID}

trait FileFixtures extends Generators {

  val uuid                  = UUID.fromString("8249ba90-7cc6-4de5-93a1-802c04200dcc")
  val uuid2                 = UUID.fromString("12345678-7cc6-4de5-93a1-802c04200dcc")
  val uuidOrg2              = UUID.fromString("66666666-7cc6-4de5-93a1-802c04200dcc")
  val ref                   = Ref.of[IO, UUID](uuid).unsafeRunSync()
  implicit val uuidF: UUIDF = UUIDF.fromRef(ref)
  val org                   = Label.unsafe("org")
  val org2                  = Label.unsafe("org2")
  val project               = ProjectGen.project(org.value, "proj", base = nxv.base, mappings = ApiMappings("file" -> schemas.files))
  val deprecatedProject     = ProjectGen.project("org", "proj-deprecated")
  val projectRef            = project.ref
  val diskId2               = nxv + "disk2"
  val file1                 = nxv + "file1"
  val file2                 = nxv + "file2"
  val fileTagged            = nxv + "fileTagged"
  val fileTagged2           = nxv + "fileTagged2"
  val file1Encoded          = encodeUriPath(file1.toString)
  val encodeId              = (id: String) => encodeUriPath((nxv + id).toString)
  val generatedId           = project.base.iri / uuid.toString
  val generatedId2          = project.base.iri / uuid2.toString

  val content = "file content"
  val path    = FileGen.mkTempDir("files")

  def withUUIDF[T](id: UUID)(test: => T): T = (for {
    old <- ref.getAndSet(id)
    t   <- IO(test).onError { case _ => ref.set(old) }
    _   <- ref.set(old)
  } yield t).unsafeRunSync()

  def attributes(
      path: AbsolutePath,
      filename: String = "file.txt",
      size: Long = 12,
      id: UUID = uuid,
      projRef: ProjectRef = projectRef,
      keywords: Map[Label, String] = Map.empty,
      description: Option[String] = None,
      name: Option[String] = None
  ): FileAttributes = FileGen.attributes(filename, size, id, projRef, path, keywords, description, name)

  def genKeywords(): Map[Label, String] = Map(Label.unsafe(genString()) -> genString())

  def genCustomMetadata(): FileCustomMetadata =
    FileCustomMetadata(Some(genString()), Some(genString()), Some(genKeywords()))

  def entity(filename: String = "file.txt"): MessageEntity =
    Multipart
      .FormData(
        Multipart.FormData.BodyPart("file", HttpEntity(`text/plain(UTF-8)`, content), Map("filename" -> filename))
      )
      .toEntity()

  def randomEntity(filename: String, size: Int): MessageEntity =
    Multipart
      .FormData(
        Multipart.FormData.BodyPart("file", HttpEntity(`text/plain(UTF-8)`, "0" * size), Map("filename" -> filename))
      )
      .toEntity()

  def base64encode(input: String) = {
    val encodedBytes = Base64.getEncoder.encode(input.getBytes("UTF-8"))
    new String(encodedBytes, "UTF-8")
  }
}
