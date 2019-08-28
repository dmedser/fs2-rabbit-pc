package app

import cats.effect.ExitCode
import monix.eval.{Task, TaskApp}

object Main extends TaskApp {
  def run(args: List[String]): Task[ExitCode] = Application.resource.use(_.run)
}
