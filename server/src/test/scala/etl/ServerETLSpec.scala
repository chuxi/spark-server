package etl

import java.io.File
import java.nio.file.{StandardCopyOption, CopyOption, Files, Paths}

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import controllers.{JobInfoController, ContextController, JarController}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSpec}
import play.api.Configuration
import play.api.libs.Files.TemporaryFile
import play.api.mvc.{Result, MultipartFormData}
import play.api.mvc.MultipartFormData.FilePart
import play.api.test.Helpers._
import play.api.test.{FakeRequest, FakeApplication, TestServer}
import services.actors.TestJarFinder
import services.{JarManager, JobInfoManager, ContextManager, InMemoryDAO}
import services.io.JobDAO

import scala.concurrent.Future

/**
  * Created by king on 15-12-16.
  * I tried use the scalatestplus play, which lead to version incompatible problem
  * and so does spark, on com.typesafe.config
  */
class ServerETLSpec(_system: ActorSystem) extends ETLBaseSpec(_system) with TestJarFinder {
  def this() = this(ActorSystem("ServerETLSpec"))

  val config = ConfigFactory.load()

  var dao: JobDAO = _

  var ctxManager: ActorRef = _
  var jobInfoManager: ActorRef = _
  var jarManager: ActorRef = _
  var jarController: JarController = _
  var ctxController: ContextController = _
  var jobController: JobInfoController = _

  val appName: String = "test"
  val contextName: String = "ctx1"
  val configString: String =
    """
      | source {
      |   type: "file"
      |   file {
      |     url: "hdfs://192.168.1.11:9000/etl/sample.txt"
      |   }
      | }
      |
      | sharedrdds: "source"
      |
      | etl {
      |   extract {
      |     table: "students"
      |     schema: "number, name, gender, age, math, physics, chemistry"
      |     sql: "select number, name, age from students where math > 90"
      |   }
      |
      |   transform {}
      |
      |   load {
      |     type: "file"
      |     url: "hdfs://192.168.1.11:9000/etl/result.parquet"
      |   }
      | }
      |
    """.stripMargin
  var classPath: String = _

  def getTempTestJar: File = {
    val fp = Paths.get("/tmp", testJar.getName)
    Files.copy(testJar.toPath, fp, StandardCopyOption.REPLACE_EXISTING)
    fp.toFile
  }

  override def beforeAll() {
    dao = new InMemoryDAO
    ctxManager = system.actorOf(Props(classOf[ContextManager], dao), "contextManager")
    jobInfoManager = system.actorOf(Props(classOf[JobInfoManager], dao, ctxManager), "jobinfoManager")
    jarManager = system.actorOf(Props(classOf[JarManager], dao))
    jarController = new JarController(jarManager)
    jobController = new JobInfoController(jobInfoManager, ctxManager, Configuration(config))
    ctxController = new ContextController(ctxManager)

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

  override def afterAll(): Unit = {
    system.stop(jarManager)
    system.stop(ctxManager)
    system.stop(jobInfoManager)
    TestKit.shutdownActorSystem(system)
  }

  describe("DataSourceApp") {
    it("should get data") {
      var data = Map(
        "appName" -> appName,
        "contextOpt" -> contextName,
        "classPath" -> "cn.edu.zju.king.jobserver.etl.DataSourceApp",
        "config" -> configString,
        "syncOpt" -> "true",
        "timeoutOpt" -> "20"
      )

      var result: Future[Result] = jobController.submitJob().apply(FakeRequest().withFormUrlEncodedBody(data.toSeq: _*))
      (contentAsJson(result) \ "status").as[String] should be ("FINISHED")

      data = Map(
        "appName" -> appName,
        "contextOpt" -> contextName,
        "classPath" -> "cn.edu.zju.king.jobserver.etl.ETLApp",
        "config" -> configString,
        "syncOpt" -> "true",
        "timeoutOpt" -> "20"
      )

      result = jobController.submitJob().apply(FakeRequest().withFormUrlEncodedBody(data.toSeq: _*))
      (contentAsJson(result) \ "status").as[String] should be ("FINISHED")

    }
  }


}
