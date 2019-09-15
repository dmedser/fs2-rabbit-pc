package app

import java.util.UUID

import app.amqp.model.PersonEvent.PersonCreate
import app.amqp.{Message, Meta}
import app.config.AppConfig
import app.util.AmqpUtil._
import cats.effect._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpMessage}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import monix.eval.{Task, TaskApp}

final case class Application[F[_] : Sync](producer: Producer[F], log: Logger[F]) {

  def run: F[ExitCode] =
    for {
      _ <- log.info("Producer")
      _ <- producer
        .publish(Message(PersonCreate(UUID.randomUUID(), "Tony", 30), Meta(PersonCreate.`type`)))
        .compile
        .drain
    } yield ExitCode.Success
}

object Application extends TaskApp {

  def run(args: List[String]): Task[ExitCode] = Application.resource.use(_.run)

  private def mkApplication[F[_] : Sync](producer: Producer[F], log: Logger[F]): F[Application[F]] =
    Application(producer, log).pure[F]

  private def resource[F[_] : ConcurrentEffect]: Resource[F, Application[F]] =
    for {
      config                          <- Resource.liftF(AppConfig.load[F])
      rabbit                          <- Resource.liftF(Fs2Rabbit[F](config.fs2Rabbit))
      connection                      <- rabbit.createConnection
      implicit0(channel: AMQPChannel) <- rabbit.createChannel(connection)
      publisher <- Resource.liftF(
        rabbit.createPublisher[AmqpMessage[String]](config.broker.exchange, config.broker.routingKey)
      )
      producer    <- Resource.liftF(Producer.create[F](publisher))
      log         <- Resource.liftF(Slf4jLogger.create[F])
      application <- Resource.liftF(mkApplication[F](producer, log))
    } yield application
}
