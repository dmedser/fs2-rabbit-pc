package app.amqp

import io.circe.generic.JsonCodec

@JsonCodec
final case class Meta(`type`: String)
