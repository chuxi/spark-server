import Dependencies._

lazy val commonSettings = Seq(
  version := "0.0.1",
  scalaVersion := "2.10.5",
  organization := "cn.edu.zju.king",
  libraryDependencies ++= Seq(
    log4j2Api,
    log4j2Core,
    scalaTest,
    akkaTest,
    sparkSQL excludeAll(excludeNettyIO, excludeMacros)
  ),
  resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  updateOptions := updateOptions.value.withCachedResolution(true)
)


def excludeJackson(module: ModuleID): ModuleID =
  module.excludeAll(excludeDatabind, excludeJacksonCore)

lazy val serverSettings = Seq(
  description := "the backend web server",
  libraryDependencies ++= Seq(
    ngFileUpload,
    angularJS,
    bootstrap
  ),
  dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.4.4",
  libraryDependencies ~= (_.map(excludeJackson)),
  parallelExecution in Test := false
)

lazy val serverapiSettings = Seq(
  description := "users run spark job by implementing the apis",
  libraryDependencies ++= Seq()
)

lazy val algolibsSettings = Seq(
  description := "some default implemented etl packages",
  libraryDependencies ++= Seq(),
  exportJars := true
)

lazy val server = Project(id = "server", base = file("server"))
  .dependsOn(serverapi)
  .settings(commonSettings: _*)
  .enablePlugins(PlayScala)
  .disablePlugins(PlayLayoutPlugin)
  .settings(serverSettings: _*)



lazy val serverapi = Project(id = "serverapi", base = file("serverapi"))
  .settings(commonSettings: _*)
  .settings(serverapiSettings: _*)

lazy val algolibs = Project(id = "algolibs", base = file("algolibs"))
  .dependsOn(serverapi)
  .settings(commonSettings: _*)
  .settings(algolibsSettings: _*)

lazy val root = Project(id = "spark-server", base = file("."))
  .aggregate(server, serverapi, algolibs)
  .settings(
    name := "spark-server"
  )