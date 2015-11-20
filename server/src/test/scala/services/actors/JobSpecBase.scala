package services.actors

import java.lang.Boolean
import java.util.Date

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.gracefulStop
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import services.contexts.DefaultSparkContextFactory
import services.io.JobDAO
import services.protocals.CommonMessages._

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Provides a base Config for tests.  Override the vals to configure.  Mix into an object.
 * Also, defaults for values not specified here could be provided as java system properties.
 */
trait JobSpecConfig {
  import collection.JavaConverters._

  val JobResultCacheSize = Integer.valueOf(30)
  val NumCpuCores = Integer.valueOf(Runtime.getRuntime.availableProcessors())  // number of cores to allocate. Required.
  val MemoryPerNode = "512m"  // Executor memory per node, -Xmx style eg 512m, 1G, etc.
  val MaxJobsPerContext = Integer.valueOf(2)
  def contextFactory = classOf[DefaultSparkContextFactory].getName
  lazy val config = {
    val ConfigMap = Map(
      "spark.jobserver.job-result-cache-size" -> JobResultCacheSize,
      "num-cpu-cores" -> NumCpuCores,
      "memory-per-node" -> MemoryPerNode,
      "spark.jobserver.max-jobs-per-context" -> MaxJobsPerContext,
      "akka.log-dead-letters" -> Integer.valueOf(0),
      "spark.master" -> "local[4]",
      "context-factory" -> contextFactory,
      "spark.context-settings.test" -> ""
    )
    ConfigFactory.parseMap(ConfigMap.asJava).withFallback(ConfigFactory.defaultOverrides())
  }

  lazy val contextConfig = {
    val ConfigMap = Map(
      "context-factory" -> contextFactory,
      "streaming.batch_interval" -> new Integer(40),
      "streaming.stopGracefully" -> new Boolean(false),
      "streaming.stopSparkContext" -> new Boolean(true)
    )
    ConfigFactory.parseMap(ConfigMap.asJava).withFallback(ConfigFactory.defaultOverrides())
  }

  def getNewSystem = ActorSystem("test", config)
}

abstract class JobSpecBaseBase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var dao: JobDAO = _
  var manager: ActorRef = _
  def testJar: java.io.File

  val timeout = 15.seconds
  after {
//    manager ! PoisonPill
    val stopped = gracefulStop(manager, timeout)
    Await.result(stopped, timeout)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  protected def uploadJar(dao: JobDAO, jarFilePath: String, appName: String) {
    val bytes = scala.io.Source.fromFile(jarFilePath, "ISO-8859-1").map(_.toByte).toArray
    dao.saveJar(appName, new Date(), bytes)
  }

  protected def uploadTestJar(appName: String = "demo") { uploadJar(dao, testJar.getAbsolutePath, appName) }

  val errorEvents: Set[Class[_]] = Set(classOf[JobErroredOut], classOf[JobValidationFailed],
    classOf[NoJobSlotsAvailable], classOf[JobKilled])
  val asyncEvents = Set(classOf[JobStarted])
  val syncEvents = Set(classOf[JobResult])
  val allEvents = errorEvents ++ asyncEvents ++ syncEvents ++ Set(classOf[JobFinished])

}

class JobSpecBase(_system: ActorSystem) extends JobSpecBaseBase(_system) with TestJarFinder
