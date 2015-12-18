package controllers

import java.io.File
import java.nio.file.{Paths, Files}

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import play.api.Configuration
import play.api.libs.Files.TemporaryFile
import play.api.mvc.MultipartFormData.FilePart
import play.api.mvc.{MultipartFormData, Result}
import play.api.test.FakeRequest
import services.{JarManager, JobInfoManager, ContextManager, InMemoryDAO}
import services.actors.TestJarFinder
import services.io.JobDAO
import play.api.test.Helpers._

import scala.concurrent.Future

/**
  * Created by king on 15-12-13.
  */
class JobControllerSpec(_system: ActorSystem) extends ControllerBaseSpec(_system) with TestJarFinder {
  def this() = this(ActorSystem("JobController"))

  val config = ConfigFactory.load()

  var dao: JobDAO = _

  var ctxManager: ActorRef = _
  var jobInfoManager: ActorRef = _
  var jarManager: ActorRef = _
  var jarController: JarController = _
  var ctxController: ContextController = _
  var jobController: JobInfoController = _

  var appName: String = _
  var contextName: String = _
  var configString: String = _
  var classPath: String = _

  override def beforeAll() {
    dao = new InMemoryDAO
    ctxManager = system.actorOf(Props(classOf[ContextManager], dao), "contextManager")
    jobInfoManager = system.actorOf(Props(classOf[JobInfoManager], dao, ctxManager), "jobinfoManager")
    jarManager = system.actorOf(Props(classOf[JarManager], dao))
    jarController = new JarController(jarManager)
    jobController = new JobInfoController(jobInfoManager, ctxManager, Configuration(config))
    ctxController = new ContextController(ctxManager)

    appName = "test"
    contextName = "ctx1"
    configString = "input.string = The lazy dog jumped over the fish"
    classPath = "cn.edu.zju.king.jobserver.test.WordCountExample"

    // upload the test jar
    val form = MultipartFormData(
      Map(),
      List(FilePart("jar", "file.jar", Some("Content-Type: multipart/form-data"), TemporaryFile(getTempTestJar))),
      List(),
      List()
    )
    var result: Future[Result] = jarController.postJar(appName).apply(FakeRequest().withMultipartFormDataBody(form))
    status(result) should be (OK)

    // create the context for submiting a job
    result= ctxController.createContext(contextName).apply(FakeRequest())
    status(result) should be (OK)
  }

//  override def afterAll(): Unit = {
//    system.stop(jarManager)
//    system.stop(ctxManager)
//    system.stop(jobInfoManager)
//    TestKit.shutdownActorSystem(system)
//  }

  def getTempTestJar: File = {
    val fp = Paths.get("/tmp", testJar.getName)
    if (!fp.toFile.exists()) {
      Files.copy(testJar.toPath, fp)
    }
    fp.toFile
  }

  describe("JobInfo Controller") {
    it("should list no job when it is empty") {
      val result: Future[Result] = jobController.listJobs(20).apply(FakeRequest())
      status(result) should be (OK)
    }

    it("should submit a job asynchronously") {
      val data = Map(
        "appName" -> appName,
        "contextOpt" -> contextName,
        "classPath" -> classPath,
        "config" -> configString
      )

      val result: Future[Result] = jobController.submitJob().apply(FakeRequest().withFormUrlEncodedBody(data.toSeq: _*))
      (contentAsJson(result) \ "status").as[String] should be ("STARTED")

      Thread.sleep(1000)
    }

    it("could submit a job synchronously") {
      val data = Map(
        "appName" -> appName,
        "contextOpt" -> contextName,
        "classPath" -> classPath,
        "config" -> configString,
        "syncOpt" -> "true"
      )

      val result: Future[Result] = jobController.submitJob().apply(FakeRequest().withFormUrlEncodedBody(data.toSeq: _*))

      val res = contentAsJson(result)
      (res \ "jobid").as[String] should not be empty
      println((res \ "result").as[String])
      Thread.sleep(1000)
    }

  }


}
