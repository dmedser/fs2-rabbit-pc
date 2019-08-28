package app.amqp

import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, Json}

final case class Message[T](data: T, meta: Meta)

object Message {

  implicit def messageDecoder[T : Decoder]: Decoder[Message[T]] = deriveDecoder
  implicit def messageEncoder[T : Encoder]: Encoder[Message[T]] = deriveEncoder

  def decodeData[T](message: Message[Json])(implicit decoder: DataDecoder[T]): Result[T] =
    decoder.decodeData(message.data, message.meta.`type`)
}
