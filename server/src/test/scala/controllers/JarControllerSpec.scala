package controllers

import java.io.File
import java.nio.file.{Paths, CopyOption, Files}
import java.util.Date

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.TestKit
import play.api.libs.Files.TemporaryFile
import play.api.libs.json
import play.api.mvc.MultipartFormData.FilePart
import play.api.mvc._
import play.api.test.{FakeHeaders, Helpers, FakeRequest}
import services.actors.TestJarFinder
import services.io.JobDAO
import services.{JarManager, InMemoryDAO}
import play.api.test.Helpers._

import scala.concurrent.Future

/**
  * Created by king on 15-11-19.
  */
class JarControllerSpec(_system: ActorSystem) extends ControllerBaseSpec(_system) with TestJarFinder {
  def this() = this(ActorSystem("JarController"))

  var dao: JobDAO = _
  var jarController: JarController = _
  var jarManager: ActorRef = _

  override def beforeAll() {
    dao = new InMemoryDAO
    jarManager = system.actorOf(Props(classOf[JarManager], dao))
    jarController = new JarController(jarManager)
  }

//  override def afterAll(): Unit = {
//    system.stop(jarManager)
//    TestKit.shutdownActorSystem(system)
//  }

  def uploadTestJar(appName: String) = {
    val bytes = scala.io.Source.fromFile(testJar.getAbsolutePath, "ISO-8859-1").map(_.toByte).toArray
    dao.saveJar(appName, new Date(), bytes)
  }

  def getTempTestJar: File = {
    val fp = Paths.get("/tmp", testJar.getName)
    if (!fp.toFile.exists()) {
      Files.copy(testJar.toPath, fp)
    }
    fp.toFile
  }

  describe("Jar Controller") {
    describe("when it is empty") {
      it("should list nothing in the server") {
        val result: Future[Result] = jarController.listJars().apply(FakeRequest())
        //      println(contentAsJson(result))
        val rs1 = contentAsJson(result)
        rs1 should equal(json.Json.parse("[]"))
      }
    }

    describe("after posted a jar") {
      it("should return the jar json in the server") {
        val appName = "demo"

        uploadTestJar(appName)

        val result: Future[Result] = jarController.listJars().apply(FakeRequest())
        val rs1 = contentAsJson(result)
        appName should equal ((rs1(0) \ "appName").as[String])
      }
    }
  }

  describe("Jar Controller posts a jar") {
    it("should store the jar successfully") {
      val appName = "demo1"

      val form = MultipartFormData(
        Map(),
        List(FilePart("jar", "file.jar", Some("Content-Type: multipart/form-data"), TemporaryFile(getTempTestJar))),
        List(),
        List()
      )

      val rq = FakeRequest("POST", "/jars").withMultipartFormDataBody(form)
      val result: Future[Result] = jarController.postJar(appName).apply(rq)
      status(result) should be (OK)
    }
  }

}
