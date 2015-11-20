package services

import java.util.Date

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest._
import services.InMemoryDAO
import services.JobInfoManagerMessages._
import services.io.{JarInfo, JobDAO, JobInfo}
import services.protocals.CommonMessages.NoSuchJobId

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by king on 15-10-9.
 */
class JobInfoManagerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  def this() = this(ActorSystem("JobInfoActorTest"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val timeout = 15.seconds
  val jobId = "jobId"
  val jobConfig = ConfigFactory.empty()


  var dao: JobDAO = _
  var actor: ActorRef = _

  before {
    dao = new InMemoryDAO
    actor = system.actorOf(Props(classOf[JobInfoManager], dao, system.actorOf(Props(classOf[ContextManager], dao))))
  }

  after {
    val stopped = akka.pattern.gracefulStop(actor, timeout)
    Await.result(stopped, timeout)
  }

  describe("JobInfoManager") {
//    it("should store a job configuration") {
//      actor ! StoreJobConfig(jobId, jobConfig)
//      expectMsg(JobConfigStored)
//      dao.getJobConfigs.get(jobId) should be (Some(jobConfig))
//    }

//    it("should return a job configuration when the jobId exists") {
//      actor ! StoreJobConfig(jobId, jobConfig)
//      expectMsg(JobConfigStored)
//      actor ! GetJobConfig(jobId)
//      expectMsg(jobConfig)
//    }

//    it("should return error if jobId does not exist") {
//      actor ! GetJobConfig(jobId)
//      expectMsg(NoSuchJobId)
//    }

    it("should return job status") {
      actor ! GetJobStatus(jobId)
      expectMsg(NoSuchJobId)

      // add a job status
      val jobinfo = JobInfo(jobId, "test", JarInfo("test", new Date()), "", system.settings.config, new Date(), None, None)
      dao.saveJobInfo(jobinfo)

      actor ! GetJobStatuses(Some(10))
      expectMsg(Seq(jobinfo))

      actor ! GetJobStatus(jobId)
      expectMsg(jobinfo)
    }

  }

}
