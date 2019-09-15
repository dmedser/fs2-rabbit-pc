package app

import app.util.AmqpUtil._
import cats.Applicative
import dev.profunktor.fs2rabbit.model._
import fs2.Stream
import io.circe.Encoder

class Producer[F[_]](publisher: AmqpMessage[String] => F[Unit]) {

  def publish[Payload : Encoder](payload: Payload, properties: AmqpProperties = AmqpProperties.empty): Stream[F, Unit] =
    Stream(AmqpMessage(payload, properties)).covary.through(jsonPipe[Payload]).evalMap(publisher)
}

object Producer {
  def create[F[_]](publisher: AmqpMessage[String] => F[Unit])(implicit F: Applicative[F]): F[Producer[F]] =
    F.pure(new Producer(publisher))
}
