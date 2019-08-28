package app

import app.amqp.Consumer
import app.config.AppConfig
import app.config.AppConfig.BrokerConfig
import app.util.AmqpUtil._
import cats.effect._
import cats.syntax.flatMap._
import cats.syntax.functor._
import dev.profunktor.fs2rabbit.config.declaration.DeclarationQueueConfig
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model.{AMQPConnection, ExchangeType}
import dev.profunktor.fs2rabbit.resiliency.ResilientStream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

final case class Application[F[_] : Sync : Timer](
  config: BrokerConfig,
  rabbit: Fs2Rabbit[F],
  connection: AMQPConnection,
  log: Logger[F]
) {

  def run: F[ExitCode] = {
    import config._
    for {
      _ <- log.info("Consumer")
      _ <- bindQueue(
        fs2Rabbit = rabbit,
        amqpConnection = connection,
        exchangeName = exchange,
        exchangeType = ExchangeType.Direct,
        queueName = queue,
        queueConfig = DeclarationQueueConfig.default(queue),
        routingKey = routingKey
      )
      _ <- ResilientStream.runF {
        Consumer(rabbit, connection, queue) >>= { consumer =>
          (for {
            event <- consumer.consumeEvent()
            _     <- consumer.handleEvent(event)
          } yield ()).compile.drain
        }
      }
    } yield ExitCode.Success
  }
}

object Application {

  private def mkApplication[F[_] : Sync : Timer](
    config: BrokerConfig,
    rabbit: Fs2Rabbit[F],
    connection: AMQPConnection,
    log: Logger[F]
  ): F[Application[F]] =
    Sync[F].delay(Application(config, rabbit, connection, log))

  def resource[F[_] : ConcurrentEffect : Timer]: Resource[F, Application[F]] =
    for {
      config      <- Resource.liftF(AppConfig.load)
      rabbit      <- Resource.liftF(Fs2Rabbit(config.fs2Rabbit))
      connection  <- rabbit.createConnection
      log         <- Resource.liftF(Slf4jLogger.create)
      application <- Resource.liftF(mkApplication(config.broker, rabbit, connection, log))
    } yield application
}
