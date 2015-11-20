package services.actors

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._
import services.protocals.CommonMessages._

/**
 * Created by king on 15-10-9.
 */
class JobResultActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  def this() = this(ActorSystem("JobResultActorSpec"))

  var actor: ActorRef = _

  override def beforeAll() {
//    actor = system.actorOf(Props[JobResultActor])
  }

  before {
    actor = system.actorOf(Props[JobResultActor])
  }

  after {
    actor ! PoisonPill
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  describe("JobResultActor") {
    it("should return error if non-existing jobs are asked") {
      actor ! GetJobResult("jobId")
      expectMsg(NoSuchJobId)
    }

    it("should get back existing result") {
      actor ! JobResult("jobId", 10)
      actor ! GetJobResult("jobId")
      expectMsg(JobResult("jobId", 10))
    }

    it("should be informed only once by subscribed result") {
      actor ! Subscribe("jobId", self, Set(classOf[JobResult]))
      actor ! JobResult("jobId", 10)
      expectMsg(JobResult("jobId", 10))

      actor ! JobResult("jobId", 20)
      expectNoMsg()   // shouldn't get it again
    }

    it("should not be informed unsubscribed result") {
      actor ! Subscribe("jobId", self, Set(classOf[JobResult]))
      actor ! Unsubscribe("jobId", self)
      actor ! JobResult("jobId", 10)
      expectNoMsg()
    }

    it("should not publish if do not subscribe to JobResult events") {
      actor ! Subscribe("jobId", self, Set(classOf[JobValidationFailed], classOf[JobErroredOut]))
      actor ! JobResult("jobId", 10)
      expectNoMsg()
    }

    it("should return error if non-existing subscription is unsubscribed") {
      actor ! Unsubscribe("jobId", self)
      expectMsg(NoSuchJobId)
    }

  }

}
