import sbt._

object Dependencies {

  object Version {
    val catsCore = "1.6.0"
    val catsEffect = "1.2.0"
    val circe = "0.10.0"
    val logback = "1.2.3"
    val log4cats = "0.2.0"
    val monix = "3.0.0-RC2"
    val fs2 = "1.0.4"
    val fs2rabbit = "2.0.0-RC2"
    val pureconfig = "0.10.2"
  }

  val catsCore = "org.typelevel"                     %% "cats-core"              % Version.catsCore
  val catsEffect = "org.typelevel"                   %% "cats-effect"            % Version.catsEffect
  val circeCore = "io.circe"                         %% "circe-core"             % Version.circe
  val circeGeneric = "io.circe"                      %% "circe-generic"          % Version.circe
  val circeParser = "io.circe"                       %% "circe-parser"           % Version.circe
  val logback = "ch.qos.logback"                     % "logback-classic"         % Version.logback
  val log4catsCore = "io.chrisdavenport"             %% "log4cats-core"          % Version.log4cats
  val log4catsSlf4j = "io.chrisdavenport"            %% "log4cats-slf4j"         % Version.log4cats
  val log4catsExtras = "io.chrisdavenport"           %% "log4cats-extras"        % Version.log4cats
  val monix = "io.monix"                             %% "monix"                  % Version.monix
  val fs2 = "co.fs2"                                 %% "fs2-core"               % Version.fs2
  val fs2rabbit = "dev.profunktor"                   %% "fs2-rabbit"             % Version.fs2rabbit
  val fs2rabbitCirce = "dev.profunktor"              %% "fs2-rabbit-circe"       % Version.fs2rabbit
  val pureConfig = "com.github.pureconfig"           %% "pureconfig"             % Version.pureconfig
  val pureConfigCatsEffect = "com.github.pureconfig" %% "pureconfig-cats-effect" % Version.pureconfig
}
