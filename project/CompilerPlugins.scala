import sbt._

object CompilerPlugins {
  object Version {
    val paradise = "2.1.1"
    val kindProjector = "0.10.0"
    val betterMonadicFor = "0.3.0"
  }

  val paradise = compilerPlugin("org.scalamacros"    % "paradise"            % Version.paradise cross CrossVersion.full)
  val kindProjector = compilerPlugin("org.typelevel" %% "kind-projector"     % Version.kindProjector)
  val betterMonadicFor = compilerPlugin("com.olegpy" %% "better-monadic-for" % Version.betterMonadicFor)
}
