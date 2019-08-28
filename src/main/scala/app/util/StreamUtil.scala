package app.util

import cats.~>
import fs2.Stream

object StreamUtil {
  def liftK[F[_]]: F ~> Stream[F, *] = λ[F ~> Stream[F, *]](Stream.eval(_))
}
