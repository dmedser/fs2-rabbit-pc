package app.util

import java.nio.charset.StandardCharsets.UTF_8

import app.amqp.{DataDecoder, Message}
import cats.data.Kleisli
import cats.effect.Sync
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.{Applicative, Monad}
import dev.profunktor.fs2rabbit.config.declaration.DeclarationQueueConfig
import dev.profunktor.fs2rabbit.effects.MessageEncoder
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.json.Fs2JsonEncoder
import dev.profunktor.fs2rabbit.model._
import fs2.{Pipe, Pure}
import io.chrisdavenport.log4cats.Logger
import io.circe.{Encoder, Json, parser}

object AmqpUtil {

  def bindQueue[F[_] : Monad](
    fs2Rabbit: Fs2Rabbit[F],
    exchangeName: ExchangeName,
    exchangeType: ExchangeType,
    queueName: QueueName,
    queueConfig: DeclarationQueueConfig,
    routingKey: RoutingKey
  )(implicit amqpChannel: AMQPChannel): F[Unit] =
    for {
      _ <- fs2Rabbit.declareExchange(exchangeName, exchangeType)
      _ <- fs2Rabbit.declareQueue(queueConfig)
      _ <- fs2Rabbit.bindQueue(queueName, exchangeName, routingKey)
    } yield ()

  implicit def stringMessageEncoder[F[_] : Applicative]: MessageEncoder[F, AmqpMessage[String]] =
    Kleisli[F, AmqpMessage[String], AmqpMessage[Array[Byte]]](
      message => message.copy(payload = message.payload.getBytes(UTF_8)).pure[F]
    )

  private val jsonEncoder = new Fs2JsonEncoder
  import jsonEncoder.jsonEncode

  def jsonPipe[A : Encoder]: Pipe[Pure, AmqpMessage[A], AmqpMessage[String]] = _.map(jsonEncode[A])

  def decodeData[T]: DecodeDataPartiallyApplied[T] = new DecodeDataPartiallyApplied[T]

  private[util] class DecodeDataPartiallyApplied[T] {
    def apply[F[_]](decodedString: String)(implicit F: Sync[F], decoder: DataDecoder[T], log: Logger[F]): F[T] =
      (for {
        decodedJson    <- F.delay(parser.decode[Message[Json]](decodedString))
        _              <- log.debug(s"Decoded json:\n$decodedJson")
        decodedMessage <- F.delay(decodedJson.flatMap(Message.decodeData[T]))
        _              <- log.debug(s"Decoded message:\n$decodedMessage")
      } yield decodedMessage)
        .widen[Either[Throwable, T]]
        .rethrow
  }
}
