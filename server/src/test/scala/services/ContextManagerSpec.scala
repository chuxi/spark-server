package services

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import services.ContextManagerMessages._
import services.io.JobDAO

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by king on 15-10-12.
 */

object ContextManagerSpec {
  val config = ConfigFactory.parseString("""
    spark {
      master = "local[4]"
      temp-contexts {
        num-cpu-cores = 4           # Number of cores to allocate.  Required.
        memory-per-node = 512m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
      }
      jobserver.job-result-cache-size = 100
      jobserver.context-creation-timeout = 5 s
      jobserver.yarn-context-creation-timeout = 40 s
      contexts {
        olap-demo {
          num-cpu-cores = 4
          memory-per-node = 512m
        }
      }

      context-settings {
        num-cpu-cores = 2
        memory-per-node = 512m
        context-factory = services.contexts.DefaultSparkContextFactory
        passthrough {
          spark.driver.allowMultipleContexts = true
          spark.ui.enabled = false
        }
      }
    }
    akka.log-dead-letters = 0 """)
}


class ContextManagerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  def this() = this(ActorSystem("LocalContextSupervisorActorTest", ContextManagerSpec.config))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  var supervisor: ActorRef = _
  var dao: JobDAO = _

  val timeout = 15.seconds
  val contextConfig = ContextManagerSpec.config.getConfig("spark.context-settings")

  // This is needed to help tests pass on some MBPs when working from home
  System.setProperty("spark.driver.host", "localhost")

  before {
    dao = new InMemoryDAO
    supervisor = system.actorOf(Props(classOf[ContextManager], dao))
  }

  after {
    val stopped = akka.pattern.gracefulStop(supervisor, timeout)
    Await.result(stopped, timeout)
  }

  describe("context management") {
    it("should list empty contexts at startup") {
      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }

    it("can add contexts from jobConfig") {
      supervisor ! AddContextsFromConfig
      Thread sleep 4000
      supervisor ! ListContexts
      expectMsg(40.seconds, Seq("olap-demo"))
    }

    it("should be able to add multiple new contexts") {
      // serializing the creation at least until SPARK-2243 gets
      // solved.
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)
      supervisor ! AddContext("c2", contextConfig)
      expectMsg(1000.seconds, ContextInitialized)
      supervisor ! ListContexts
      expectMsg(Seq("c1", "c2"))
      supervisor ! GetResultActor("c1")
      val rActor = expectMsgClass(classOf[ActorRef])
      rActor.path.toString should endWith ("result-actor")
      rActor.path.toString should not include ("global")
    }

    it("should be able to stop contexts already running") {
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)
      supervisor ! ListContexts
      expectMsg(Seq("c1"))

      supervisor ! StopContext("c1")
      expectMsg(ContextStopped)

      Thread.sleep(2000) // wait for a while since deleting context is an asyc call
      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }

    it("should return NoSuchContext if attempt to stop nonexisting context") {
      supervisor ! StopContext("c1")
      expectMsg(NoSuchContext)
    }

    it("should not allow creation of an already existing context") {
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)

      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextAlreadyExists)
    }
  }



}