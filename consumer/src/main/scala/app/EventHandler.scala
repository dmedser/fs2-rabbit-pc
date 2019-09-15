package app

import app.amqp.model.PersonEvent
import app.amqp.model.PersonEvent._
import app.util.AmqpUtil._
import cats.effect.Sync
import cats.syntax.flatMap._
import cats.syntax.functor._
import dev.profunktor.fs2rabbit.model.AckResult.Ack
import dev.profunktor.fs2rabbit.model.{AckResult, AmqpEnvelope}
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class EventHandler[F[_]](consumer: Stream[F, AmqpEnvelope[String]], acker: AckResult => F[Unit])(
  implicit F: Sync[F],
  log: Logger[F]
) {

  def process(): Stream[F, Unit] =
    consumer
      .evalMap { envelope =>
        for {
          _     <- log.debug(s"Consumed:\n${envelope.payload}")
          event <- decodeData[PersonEvent](envelope.payload)
          _     <- handleEvent(event)
          _     <- acker(Ack(envelope.deliveryTag))
        } yield ()
      }

  private def handleEvent(event: PersonEvent): F[Unit] =
    log
      .info {
        event match {
          case _: PersonCreate => PersonCreate.`type`
        }
      }
}

object EventHandler {
  def create[F[_] : Sync](consumer: Stream[F, AmqpEnvelope[String]], acker: AckResult => F[Unit]): F[EventHandler[F]] =
    for {
      implicit0(log: Logger[F]) <- Slf4jLogger.create
    } yield new EventHandler(consumer, acker)
}
