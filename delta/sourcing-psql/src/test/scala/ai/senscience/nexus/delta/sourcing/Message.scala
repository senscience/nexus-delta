package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sourcing.Message.MessageRejection.MessageTooLong
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.State.EphemeralState
import cats.effect.IO
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

object Message {
  val entityType: EntityType = EntityType("message")

  def evaluate(c: CreateMessage): IO[MessageState] =
    IO.raiseWhen(c.text.length > 10)(MessageTooLong(c.id, c.project))
      .as(MessageState(c.id, c.project, c.text, c.from, Instant.EPOCH, Anonymous))

  final case class CreateMessage(id: Iri, project: ProjectRef, text: String, from: Subject)

  final case class MessageState(
      id: Iri,
      project: ProjectRef,
      text: String,
      from: Subject,
      createdAt: Instant,
      createdBy: Subject
  ) extends EphemeralState {
    override def schema: ResourceRef = Latest(schemas + "message.json")

    override def types: Set[Iri] = Set(nxv + "Message")
  }

  sealed trait MessageRejection extends Rejection {
    override def reason: String = "Something bad happened."
  }

  object MessageRejection {
    trait NotFound                                                extends MessageRejection
    final case object NotFound                                    extends NotFound
    final case class AlreadyExists(id: Iri, project: ProjectRef)  extends MessageRejection
    final case class MessageTooLong(id: Iri, project: ProjectRef) extends MessageRejection
  }

  object MessageState {

    val serializer: Serializer[Iri, MessageState] = {
      import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
      implicit val configuration: Configuration        = Configuration.default.withDiscriminator("@type")
      implicit val coder: Codec.AsObject[MessageState] = deriveConfiguredCodec[MessageState]
      Serializer()
    }
  }

}
