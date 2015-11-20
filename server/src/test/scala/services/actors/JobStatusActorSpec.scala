package services.actors

import java.util.Date

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import services.InMemoryDAO
import services.io.{JobDAO, JobInfo, JarInfo}
import services.protocals.CommonMessages._

/**
 * Created by king on 15-10-9.
 */
class JobStatusActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  def this() = this(ActorSystem("JobStatusActorSpec"))

  private val jobId = "jobId"
  private val contextName = "contextName"
  private val appName = "appName"
  private val jarInfo = JarInfo(appName, new Date())
  private val classPath = "classPath"
  private val jobConfig = ConfigFactory.empty()
  private val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, jobConfig, new Date(), None, None)


  var actor: ActorRef = _
  var dao: JobDAO = _

  override def beforeAll(): Unit = {
    dao = new InMemoryDAO
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  before {
    actor = system.actorOf(Props(classOf[JobStatusActor], dao))
  }

  after {
    actor ! PoisonPill
  }

  describe("JobStatusActor") {
    it("should return empty sequence if there is no job infos") {
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }

    it("should return error if non-existing job is unsubscribed") {
      actor ! Unsubscribe(jobId, self)
      expectMsg(NoSuchJobId)
    }

    it("should not initialize a job more than two times") {
      actor ! JobInit(jobInfo)
      actor ! JobInit(jobInfo)
      expectMsg(JobInitAlready)
    }

    it("should be informed JobStarted until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! Subscribe(jobId, self, Set(classOf[JobStarted]))
      val msg = JobStarted(jobId, contextName, new Date())
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(msg)

      actor ! Unsubscribe(jobId, self)
      actor ! JobStarted(jobId, contextName, new Date())
      expectNoMsg()   // shouldn't get it again

      actor ! Unsubscribe(jobId, self)
      expectMsg(NoSuchJobId)
    }

    it("should be ok to subscribe beofore job init") {
      actor ! Subscribe(jobId, self, Set(classOf[JobStarted]))
      actor ! JobInit(jobInfo)
      val msg = JobStarted(jobId, contextName, new Date())
      actor ! msg
      expectMsg(msg)
    }

    it("should be informed JobValidationFailed once") {
      actor ! JobInit(jobInfo)
      actor ! Subscribe(jobId, self, Set(classOf[JobValidationFailed]))
      val msg = JobValidationFailed(jobId, new Date(), new Throwable)
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(NoSuchJobId)
    }

    it("should be informed JobFinished until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! JobStarted(jobId, contextName, new Date())
      actor ! Subscribe(jobId, self, Set(classOf[JobFinished]))
      val msg = JobFinished(jobId, new Date())
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(NoSuchJobId)
    }

    it("should be informed JobErroredOut until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! JobStarted(jobId, contextName, new Date())
      actor ! Subscribe(jobId, self, Set(classOf[JobErroredOut]))
      val msg = JobErroredOut(jobId, new Date(), new Throwable)
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(NoSuchJobId)
    }

    it("should update status correctly") {
      actor ! JobInit(jobInfo)
      actor ! GetRunningJobStatus
      expectMsg(Seq(jobInfo))

      val startTime = new Date()
      actor ! JobStarted(jobId, contextName, startTime)
      actor ! GetRunningJobStatus
      expectMsg(Seq(JobInfo(jobId, contextName, jarInfo, classPath, jobConfig, startTime, None, None)))

      val finishTIme = new Date()
      actor ! JobFinished(jobId, finishTIme)
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }

    it("should update JobValidationFailed status correctly") {
      val initTime = new Date()
      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, jobConfig, initTime, None, None)
      actor ! JobInit(jobInfo)

      val failedTime = new Date()
      val err = new Throwable
      actor ! JobValidationFailed(jobId, failedTime, err)
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }

    it("should update JobErroredOut status correctly") {
      actor ! JobInit(jobInfo)

      val startTime = new Date()
      actor ! JobStarted(jobId, contextName, startTime)

      val failedTime = new Date()
      val err = new Throwable
      actor ! JobErroredOut(jobId, failedTime, err)
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }
  }

}
