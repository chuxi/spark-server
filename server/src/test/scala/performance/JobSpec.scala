package performance

import java.io.File
import java.nio.file.{StandardCopyOption, Files, Paths}

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import controllers.{JobInfoController, ContextController, JarController}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSpecLike}
import play.api.Configuration
import play.api.libs.Files.TemporaryFile
import play.api.mvc.MultipartFormData.FilePart
import play.api.mvc.{Result, MultipartFormData, Results}
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.actors.TestJarFinder
import services.{JarManager, JobInfoManager, ContextManager, InMemoryDAO}
import services.io.JobDAO

import scala.collection.mutable
import scala.concurrent.Future

/**
  * Created by king on 15-12-18.
  */
class JobSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfterAll with Results with TestJarFinder {
  import scala.concurrent.duration._
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
  val contextName: String = "JobPerformanceSpec"
  // must specify the element: partitions=2|4|8
  var configString: String = _
  val classPath: String = "cn.edu.zju.king.jobserver.performance.JoinJobApp"

  val jobTimes = mutable.LinkedHashMap[String, Long]()

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
    val result: Future[Result] = jarController.postJar(appName).apply(FakeRequest().withMultipartFormDataBody(form))
    status(result) should be (OK)
  }

  override def afterAll(): Unit = {
    Thread.sleep(1000)
    system.stop(jarManager)
    system.stop(ctxManager)
    system.stop(jobInfoManager)
    TestKit.shutdownActorSystem(system)
    jobTimes.foreach(println)
  }

  def getRequest(cores: Int, mem: String) =
    FakeRequest("POST", s"/contexts/$contextName?num-cpu-cores=$cores&memory-per-node=$mem")

  def runJob(parts: Int, mem: String): Unit = {
    // create the context
    var result: Future[Result] = ctxController.createContext(contextName).apply(getRequest(parts, mem))
    status(result) should be (OK)

    // running the job
    configString =
      s"""
        |partitions = $parts
      """.stripMargin
    val data = Map(
      "appName" -> appName,
      "contextOpt" -> contextName,
      "classPath" -> classPath,
      "config" -> configString,
      "syncOpt" -> "true",
      "timeoutOpt" -> "2000"
    )

    result = jobController.submitJob().apply(FakeRequest().withFormUrlEncodedBody(data.toSeq: _*))
    val res = contentAsJson(result)(timeout = 2000.seconds)
    (res \ "status").as[String] should be ("FINISHED")
    println(res)
    // store the result
    jobTimes += (s"$parts, $mem" -> (res \ "result").as[String].toLong)

    // stop the context
    result = ctxController.stopContext(contextName).apply(FakeRequest())
    status(result) should be (OK)

    Thread.sleep(2000)
  }


  describe("Job") {
    describe("when decrease the num-cpu-cores and keep memory-per-node = 4g") {
      it("is running under context which num-cpu-cores = 12, get the time cost") {
        runJob(12, "4g")
      }

      it("is running under context which num-cpu-cores = 10, get the time cost") {
        runJob(10, "4g")
      }

      it("is running under context which num-cpu-cores = 8, get the time cost") {
        runJob(8, "4g")
      }

      it("is running under context which num-cpu-cores = 6, get the time cost") {
        runJob(6, "4g")
      }

      it("is running under context which num-cpu-cores = 4, get the time cost") {
        runJob(4, "4g")
      }

      it("is running under context which num-cpu-cores = 2, get the time cost") {
        runJob(2, "4g")
      }
    }

    describe("when decrease the num-cpu-cores and keep memory-per-node = 512m") {
      it("is running under context which num-cpu-cores = 12, get the time cost") {
        runJob(12, "512m")
      }

      it("is running under context which num-cpu-cores = 10, get the time cost") {
        runJob(10, "512m")
      }

      it("is running under context which num-cpu-cores = 8, get the time cost") {
        runJob(8, "512m")
      }

      it("is running under context which num-cpu-cores = 6, get the time cost") {
        runJob(6, "512m")
      }

      it("is running under context which num-cpu-cores = 4, get the time cost") {
        runJob(4, "512m")
      }

      it("is running under context which num-cpu-cores = 2, get the time cost") {
        runJob(2, "512m")
      }
    }

    describe("when decrease the memory-per-node and keep num-cpu-cores = 12") {
      it("is running under context which memory-per-node = 2g, get the time cost") {
        runJob(12, "2g")
      }

      it("is running under context which memory-per-node = 1g, get the time cost") {
        runJob(12, "1g")
      }
    }

    describe("when decrease the memory-per-node and keep num-cpu-cores = 2") {
      it("is running under context which memory-per-node = 2g, get the time cost") {
        runJob(2, "2g")
      }

      it("is running under context which memory-per-node = 1g, get the time cost") {
        runJob(2, "1g")
      }
    }
  }




}
