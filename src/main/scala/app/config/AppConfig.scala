package app.config

import app.config.AppConfig._
import cats.effect.Sync
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.{ExchangeName, QueueName, RoutingKey}
import pureconfig.generic.auto._
import pureconfig.generic.ProductHint
import pureconfig.module.catseffect.loadConfigF
import pureconfig.{CamelCase, ConfigFieldMapping}

final case class AppConfig(fs2Rabbit: Fs2RabbitConfig, broker: BrokerConfig)

object AppConfig {

  def load[F[_] : Sync]: F[AppConfig] = {
    implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    loadConfigF[F, AppConfig]("app")
  }

  final case class BrokerConfig(exchange: ExchangeName, queue: QueueName, routingKey: RoutingKey)
}
