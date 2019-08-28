lazy val root = project
  .settings(name := "fs2-rabbit-pc", version := "0.1", commonSettings) in file(".")

lazy val producer = project
  .settings(name := "producer", version := "0.1", commonSettings)
  .dependsOn(root)

lazy val consumer = project
  .settings(name := "consumer", version := "0.1", commonSettings)
  .dependsOn(root)

val commonScalacOptions = Seq(
  "-encoding",
  "utf8",
  "-Xfatal-warnings",
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-language:experimental.macros"
)

val commonResolvers = Resolver.sonatypeRepo("releases")

val commonDependencies = {
  import Dependencies._
  Seq(
    catsCore,
    catsEffect,
    circeCore,
    circeGeneric,
    circeParser,
    logback,
    log4catsCore,
    log4catsSlf4j,
    log4catsExtras,
    monix,
    fs2,
    fs2rabbit,
    fs2rabbitCirce,
    pureConfig,
    pureConfigCatsEffect
  )
}

val commonCompilerPlugins = {
  import CompilerPlugins._
  Seq(paradise, kindProjector, betterMonadicFor)
}

val commonSettings = Seq(
  scalaVersion := "2.12.8",
  scalacOptions ++= commonScalacOptions,
  resolvers += commonResolvers,
  libraryDependencies ++= commonDependencies ++ commonCompilerPlugins
)