package app.amqp

import app.util.AmqpUtil._
import cats.Applicative
import cats.effect.Sync
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model._
import fs2.Stream
import io.circe.Encoder

class Producer[F[_] : Applicative](rabbit: Fs2Rabbit[F], connection: AMQPConnection) {

  def publish[Payload : Encoder](
    payload: Payload,
    exchangeName: ExchangeName,
    routingKey: RoutingKey
  ): Stream[F, Unit] =
    for {
      implicit0(channel: AMQPChannel) <- Stream.resource(rabbit.createChannel(connection))
      publisher                       <- Stream.eval(rabbit.createPublisher[AmqpMessage[String]](exchangeName, routingKey))
      message = AmqpMessage(payload = payload, properties = AmqpProperties.empty)
      _ <- Stream(message).covary[F].through(jsonPipe[Payload]).evalMap(publisher)
    } yield ()
}

object Producer {
  def apply[F[_] : Sync](rabbit: Fs2Rabbit[F], connection: AMQPConnection): F[Producer[F]] =
    Sync[F].delay(new Producer(rabbit, connection))
}
