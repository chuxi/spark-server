package services.actors

import java.net.{URI, URL}
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{PoisonPill, Props, Actor, ActorRef}
import cn.edu.zju.king.serverapi.{SparkJobValid, SparkJobInvalid, NamedRddSupport}
import com.typesafe.config.Config
import java.util.concurrent.Executors._
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkEnv
import services.ContextManager.StopContext
import services.actors.JobManagerActor._
import services.contexts.{ContextLike, SparkContextFactory}
import services.io.{JobInfo, JarInfo, JobDAO}
import services.util._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by king on 15-11-17.
  */
object JobManagerActor {
  case object Initialize
  case class StartJob(appName: String, classPath: String, config: Config, subscribedEvents: Set[Class[_]])

  case class KillJob(jobId: String)
  case object SparkContextStatus

  // results/ Data
  case class Initialized(resultActor: ActorRef)
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)
  case object SparkContextAlive
  case object SparkContextDead
}

class JobManagerActor(dao: JobDAO, contextName: String,
                contextConfig: Config, isAdHoc: Boolean = false,
                 resultActorRef: Option[ActorRef] = None) extends Actor {
  import collection.JavaConverters._
  import scala.util.control.Breaks._
  import services.protocals.CommonMessages._

  private val logger = LogManager.getLogger(getClass)

  val config = context.system.settings.config
  val sparkMaster = config.getString("spark.master")
  private val maxRunningJobs = SparkJobUtils.getMaxRunningJobs(config)
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxRunningJobs))

  var jobContext: ContextLike = null
  var sparkEnv: SparkEnv = _
  protected var rddManagerActor: ActorRef = _

  private val currentRunningJobs = new AtomicInteger(0)
  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)
  lazy val jobCache = new JobCache(jobCacheSize, dao, jobContext.sparkContext, jarLoader)

  private val statusActor = context.actorOf(Props(classOf[JobStatusActor], dao), "job-status-actor")
  protected val resultActor = resultActorRef.getOrElse(context.actorOf(Props(classOf[JobResultActor]), "job-result-actor"))

  override def postStop(): Unit = {
    logger.info(s"Shutting down SparkContext $contextName")
    Option(jobContext).foreach(_.stop())
  }

  override def receive: Receive = {
    case Initialize =>
      try {
        getSideJars(contextConfig).foreach(
          jarUri => jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri))))
        jobContext = createContextFromConfig()
        sparkEnv = SparkEnv.get
        rddManagerActor = context.actorOf(Props(classOf[RddManagerActor], jobContext.sparkContext), "rdd-manager-actor")
        getSideJars(contextConfig).foreach{ jarUri => jobContext.sparkContext.addJar(jarUri)}
        sender ! Initialized(resultActor)
      } catch {
        case t: Throwable =>
          logger.error(s"Failed to create context $contextName, shutting down actor by $t")
          sender ! InitError(t)
          self ! PoisonPill
      }

    case StartJob(appName, classPath, jobConfig, events) =>
      startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv, rddManagerActor)

    case KillJob(jobId: String) =>
      jobContext.sparkContext.cancelJobGroup(jobId)
      statusActor ! JobKilled(jobId, new Date())

    case SparkContextStatus =>
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          jobContext.sparkContext.getSchedulingMode
          sender ! SparkContextAlive
        } catch {
          case e: Exception =>
            logger.error("SparkContext is not exist!")
            sender ! SparkContextDead
        }
      }
  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       jobContext: ContextLike,
                       sparkEnv: SparkEnv,
                       rddManagerActor: ActorRef): Option[Future[Any]] = {
    var future: Option[Future[Any]] = None
    breakable {
      val lastUploadTime = dao.getApps.get(appName)
      if (lastUploadTime.isEmpty) {
        sender ! NoSuchApplication
        break()
      }

      val jarInfo = JarInfo(appName, lastUploadTime.get)
      val jobId = java.util.UUID.randomUUID().toString
      logger.info(s"Loading class $classPath for app $appName")
      val jobJarInfo = try {
        jobCache.getSparkJob(jarInfo.appName, jarInfo.uploadTime, classPath)
      } catch {
        case _: ClassNotFoundException =>
          sender ! NoSuchClass
          postEachJob()
          break()
          null

        case err: Throwable =>
          sender ! JobLoadingError(err)
          postEachJob()
          break()
          null
      }

      val job = jobJarInfo.constructor()
      if (!jobContext.isValidJob(job)) {
        sender ! WrongJobType
        break()
      }

      resultActor ! Subscribe(jobId, sender(), events)
      statusActor ! Subscribe(jobId, sender(), events)

      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, jobConfig, new Date(), None, None)
      future =
        Option(getJobFuture(jobJarInfo, jobInfo, jobConfig, sender, jobContext, sparkEnv,
          rddManagerActor))
    }
    future
  }

  private def getJobFuture(jobJarInfo: JobJarInfo,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           jobContext: ContextLike,
                           sparkEnv: SparkEnv,
                           rddManagerActor: ActorRef): Future[Any] = {
    val jobId = jobInfo.jobId
    val constructor = jobJarInfo.constructor
    logger.info(s"Starting Spark job $jobId [${jobJarInfo.className}] ...")

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, send an error message to the sender, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      sender ! NoJobSlotsAvailable(maxRunningJobs)
      return Future[Any](None)(context.dispatcher)
    }

    Future{
      logger.info("Starting job future thread")
      // Need to re-set the SparkEnv because it's thread-local and the Future runs on a diff thread
      SparkEnv.set(sparkEnv)
      Thread.currentThread.setContextClassLoader(jarLoader)

      val job = constructor()
      job match {
        case j: NamedRddSupport =>
          val namedRdds = job.asInstanceOf[NamedRddSupport].namedRddsPrivate
          if (namedRdds.get() == null) {
            namedRdds.compareAndSet(null, new JobServerNamedRdds(rddManagerActor))
          }
        case _ =>
      }

      try {
        statusActor ! JobInit(jobInfo)
        val jobC = jobContext.asInstanceOf[job.C]
        job.validate(jobC, jobConfig) match {
          case SparkJobInvalid(reason) =>
            val err = new Throwable(reason)
            statusActor ! JobValidationFailed(jobId, new Date(), err)
            throw err

          case SparkJobValid =>
            statusActor ! JobStarted(jobId: String, contextName, jobInfo.startTime)
            val sc = jobContext.sparkContext
            sc.setJobGroup(jobId, s"Job group for $jobId and spark context ${sc.applicationId}", true)
            job.runJob(jobC, jobConfig)
        }
      }
    }(executionContext).andThen {
      case Success(result: Any) =>
        statusActor ! JobFinished(jobId, new Date())
        resultActor ! JobResult(jobId, result)

      case Failure(error: Throwable) =>
        statusActor ! JobErroredOut(jobId, new Date(), error)
        logger.warn(s"Exception from job " + jobId + s": $error")
    }(executionContext).andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        currentRunningJobs.getAndDecrement()
        postEachJob()
    }(executionContext)

  }

  def createContextFromConfig(contextName: String = contextName): ContextLike = {
    val factoryClassName = contextConfig.getString("context-factory")
    val factoryClass = jarLoader.loadClass(factoryClassName)
    val factory = factoryClass.newInstance().asInstanceOf[SparkContextFactory]
    Thread.currentThread().setContextClassLoader(jarLoader)
    factory.makeContext(sparkMaster, contextName, contextConfig)
  }

  private def postEachJob(): Unit = {
    // Delete the JobManagerActor after each adhoc job
    if (isAdHoc) context.parent ! StopContext(contextName)
  }

  private def convertJarUriSparkToJava(jarUri: String): String = {
    val uri = new URI(jarUri)
    uri.getScheme match {
      case "local" => "file://" + uri.getPath
      case _ => jarUri
    }
  }

  // "Side jars" are jars besides the main job jar that are needed for running the job.
  // They are loaded from the context/job config.
  // Each one should be an URL (http, ftp, hdfs, local, or file). local URLs are local files
  // present on every node, whereas file:// will be assumed only present on driver node
  private def getSideJars(config: Config): Seq[String] = {
    Try(config.getStringList("dependent-jar-uris").asScala.toSeq)
      .orElse(Try(config.getString("dependent-jar-uris").split(",").toSeq)).getOrElse(Nil)
  }

}
