package app

import java.util.UUID

import app.amqp.model.PersonEvent.PersonCreate
import app.amqp.{Message, Meta, Producer}
import app.config.AppConfig
import app.config.AppConfig.BrokerConfig
import app.util.AmqpUtil._
import cats.effect._
import cats.syntax.flatMap._
import cats.syntax.functor._
import dev.profunktor.fs2rabbit.config.declaration.DeclarationQueueConfig
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model.{AMQPConnection, ExchangeType}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

final case class Application[F[_] : Sync](
  config: BrokerConfig,
  rabbit: Fs2Rabbit[F],
  connection: AMQPConnection,
  producer: Producer[F],
  log: Logger[F]
) {

  def run: F[ExitCode] = {
    import config._
    for {
      _ <- log.info("Producer")
      _ <- bindQueue(
        fs2Rabbit = rabbit,
        amqpConnection = connection,
        exchangeName = exchange,
        exchangeType = ExchangeType.Direct,
        queueName = queue,
        queueConfig = DeclarationQueueConfig.default(queue),
        routingKey = routingKey
      )
      message = Message(
        data = PersonCreate(id = UUID.randomUUID(), name = "Tony", age = 30),
        meta = Meta(`type` = PersonCreate.`type`)
      )
      _ <- producer.publish(message, exchange, routingKey).compile.drain
    } yield ExitCode.Success
  }
}

object Application {

  private def mkApplication[F[_] : Sync](
    config: BrokerConfig,
    rabbit: Fs2Rabbit[F],
    connection: AMQPConnection,
    producer: Producer[F],
    log: Logger[F]
  ): F[Application[F]] =
    Sync[F].delay(Application(config, rabbit, connection, producer, log))

  def resource[F[_] : ConcurrentEffect]: Resource[F, Application[F]] =
    for {
      config      <- Resource.liftF(AppConfig.load)
      rabbit      <- Resource.liftF(Fs2Rabbit(config.fs2Rabbit))
      connection  <- rabbit.createConnection
      producer    <- Resource.liftF(Producer(rabbit, connection))
      log         <- Resource.liftF(Slf4jLogger.create)
      application <- Resource.liftF(mkApplication(config.broker, rabbit, connection, producer, log))
    } yield application
}
