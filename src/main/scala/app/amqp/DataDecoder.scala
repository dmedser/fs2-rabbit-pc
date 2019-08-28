package app.amqp

import io.circe.Decoder.Result
import io.circe.Json

trait DataDecoder[T] {
  def decodeData(json: Json, `type`: String): Result[T]
}
