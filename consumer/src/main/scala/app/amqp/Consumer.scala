package app.amqp

import app.amqp.model.PersonEvent
import app.amqp.model.PersonEvent._
import app.util.AmqpUtil._
import app.util.StreamUtil._
import cats.effect.Sync
import cats.syntax.functor._
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AMQPConnection, QueueName}
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.extras.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class Consumer[F[_] : Sync](rabbit: Fs2Rabbit[F], connection: AMQPConnection, queue: QueueName)(
  implicit log: Logger[F]
) {

  def consumeEvent(): Stream[F, PersonEvent] =
    for {
      implicit0(channel: AMQPChannel) <- Stream.resource(rabbit.createChannel(connection))
      consumer                        <- Stream.eval(rabbit.createAutoAckConsumer[String](queue))
      message                         <- consumer.through(logPipe(log)).map(_.payload)
      event                           <- Stream.eval(decodeData[PersonEvent](message))
    } yield event

  def handleEvent(event: PersonEvent): Stream[F, Unit] =
    log
      .mapK[Stream[F, *]](liftK)
      .info {
        event match {
          case _: PersonCreate => PersonCreate.`type`
        }
      }
}

object Consumer {
  def apply[F[_] : Sync](rabbit: Fs2Rabbit[F], connection: AMQPConnection, queue: QueueName): F[Consumer[F]] =
    for {
      implicit0(log: Logger[F]) <- Slf4jLogger.create
    } yield new Consumer[F](rabbit, connection, queue)
}
