package app

import app.config.AppConfig
import app.config.AppConfig.BrokerConfig
import app.util.AmqpUtil._
import cats.effect._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import dev.profunktor.fs2rabbit.config.declaration.DeclarationQueueConfig
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpMessage, ExchangeType}
import dev.profunktor.fs2rabbit.resiliency.ResilientStream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import monix.eval.{Task, TaskApp}

final case class Application[F[_]](
  config: BrokerConfig,
  rabbit: Fs2Rabbit[F],
  eventHandler: EventHandler[F],
  log: Logger[F]
)(implicit F: Sync[F], timer: Timer[F], channel: AMQPChannel) {

  def run: F[ExitCode] =
    for {
      _ <- log.info("Consumer")
      _ <- bindQueue(
        rabbit,
        config.exchange,
        ExchangeType.Direct,
        config.queue,
        DeclarationQueueConfig.default(config.queue),
        config.routingKey
      )
      _ <- ResilientStream.run(eventHandler.process())
    } yield ExitCode.Success
}

object Application extends TaskApp {

  def run(args: List[String]): Task[ExitCode] = Application.resource.use(_.run)

  private def mkApplication[F[_]](
    config: BrokerConfig,
    rabbit: Fs2Rabbit[F],
    eventHandler: EventHandler[F],
    log: Logger[F]
  )(implicit F: Sync[F], timer: Timer[F], channel: AMQPChannel): F[Application[F]] =
    Application(config, rabbit, eventHandler, log).pure[F]

  private def resource[F[_] : ConcurrentEffect : Timer]: Resource[F, Application[F]] =
    for {
      config                          <- Resource.liftF(AppConfig.load[F])
      rabbit                          <- Resource.liftF(Fs2Rabbit[F](config.fs2Rabbit))
      connection                      <- rabbit.createConnection
      implicit0(channel: AMQPChannel) <- rabbit.createChannel(connection)
      (acker, consumer)               <- Resource.liftF(rabbit.createAckerConsumer[String](config.broker.queue))
      eventHandler                    <- Resource.liftF(EventHandler.create[F](consumer, acker))
      log                             <- Resource.liftF(Slf4jLogger.create[F])
      application                     <- Resource.liftF(mkApplication[F](config.broker, rabbit, eventHandler, log))
    } yield application
}
