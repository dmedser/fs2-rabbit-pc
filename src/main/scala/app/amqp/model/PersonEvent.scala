package app.amqp.model

import java.util.UUID

import app.amqp.{DataDecoder, Meta}
import app.util.ClassUtil._
import io.circe.Decoder
import io.circe.generic.JsonCodec

sealed trait PersonEvent

object PersonEvent {

  @JsonCodec
  final case class PersonCreate(id: UUID, name: String, age: Int) extends PersonEvent

  object PersonCreate {
    val `type`: String = nameOf[PersonCreate]
  }

  implicit val personEventDecoder: DataDecoder[PersonEvent] =
    (json, `type`) =>
      `type` match {
        case PersonCreate.`type` => Decoder[PersonCreate].decodeJson(json)
      }

  def deriveMeta(event: PersonEvent): Meta =
    event match {
      case _: PersonCreate => Meta(PersonCreate.`type`)
    }
}
