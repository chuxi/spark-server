import sbt._

object Dependencies {

  /**
    * commons
    */
  val log4j2Version     = "2.3"
  val log4j2Core        = "org.apache.logging.log4j"      %   "log4j-core"      % log4j2Version
  val log4j2Api         = "org.apache.logging.log4j"      %   "log4j-api"       % log4j2Version


  /**
    * exclusions from spark
    */
  val excludeNettyIO    = ExclusionRule(organization = "io.netty", artifact = "netty-all")
  val excludeScalaTest  = ExclusionRule(organization = "org.scalatest")
  val excludeScala      = ExclusionRule(organization = "org.scala-lang")
  val excludeMacros     = ExclusionRule(organization = "org.scalamacros")
  val excludeJacksonCore= ExclusionRule(organization = "com.fasterxml.jackson.core")
  val excludeDatabind   = ExclusionRule(organization = "com.fasterxml.jackson.databind")

  /**
    * spark
    */
  val sparkVersion      = "1.5.1"
  val sparkSQL          = "org.apache.spark"              %%  "spark-sql"       % sparkVersion

  val sparkDeps = Seq(
    sparkSQL excludeAll(excludeNettyIO, excludeMacros),
    "io.netty" % "netty-all" % "4.0.23.Final"
  )


  /**
   * web service play
   */
  val playVersion       = "2.4.3"
  val play              = "com.typesafe.play"             %%  "play"            % playVersion

  /**
    * webjar
    */
  val angularJSVersion  = "1.4.7"
  val bootstrapVersion  = "3.3.5"
  val angularJS         = "org.webjars"                   %   "angularjs"       % angularJSVersion
  val bootstrap         = "org.webjars"                   %   "bootstrap"       % bootstrapVersion

  /**
   * test dependency packages
   */
  val scalaTestVersion  = "2.2.5"
  val akkaTestVersion   = "2.3.14"
  val scalaTest         = "org.scalatest"                 %%  "scalatest"       % scalaTestVersion  % "test"
  val akkaTest          = "com.typesafe.akka"             %%  "akka-testkit"    % akkaTestVersion   % "test"



}