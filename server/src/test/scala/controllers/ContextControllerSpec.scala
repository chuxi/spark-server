package controllers

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.TestKit
import play.api.mvc.Result
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.{ContextManager, InMemoryDAO}
import services.io.JobDAO

import scala.concurrent.Future

/**
  * Created by king on 15-12-13.
  */
class ContextControllerSpec(_system: ActorSystem) extends ControllerBaseSpec(_system) {
  def this() = this(ActorSystem("ContextController"))

  var dao: JobDAO = _
  var contextManager: ActorRef = _
  var ctxController: ContextController = _

  override def beforeAll(): Unit = {
    dao = new InMemoryDAO
    contextManager = system.actorOf(Props(classOf[ContextManager], dao), "ctxManager")
    ctxController = new ContextController(contextManager)
  }

//  override def afterAll(): Unit = {
//    system.stop(contextManager)
//    TestKit.shutdownActorSystem(system)
//  }

  describe("Context Controller") {
    describe("when no contexts") {
      it("should list no contexts") {
        val result: Future[Result] = ctxController.listContexts().apply(FakeRequest())
        contentAsJson(result).as[Seq[String]] should equal(Seq())
      }
    }

    describe("after created a context") {
      it("should return ok and can not create another with a same contextname, at last, could be cancelled") {
        val contextName = "ctx1"
        var result: Future[Result] = ctxController.createContext(contextName).apply(FakeRequest())
        status(result) should be (OK)

        result = ctxController.listContexts().apply(FakeRequest())
        contentAsJson(result).as[Seq[String]] should equal(Seq(contextName))

        result = ctxController.createContext(contextName).apply(FakeRequest())
        status(result) should be (BAD_REQUEST)

        result = ctxController.stopContext(contextName).apply(FakeRequest())
        status(result) should be (OK)

        Thread.sleep(1000)
      }
    }

    it("could create multi-contexts") {
      val contextName1 = "ctx2"
      val contextName2 = "ctx3"

      var result: Future[Result] = ctxController.createContext(contextName1).apply(FakeRequest())
      status(result) should be (OK)

      result = ctxController.createContext(contextName2).apply(FakeRequest())
      status(result) should be (OK)

      result = ctxController.listContexts().apply(FakeRequest())
      contentAsJson(result).as[Seq[String]].sorted should equal(Seq(contextName1, contextName2).sorted)

      Thread.sleep(1000)
    }

  }

}
