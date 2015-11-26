package controllers

import akka.actor.ActorRef
import akka.util.Timeout
import com.google.inject.name.Named
import com.google.inject.{Inject, Singleton}
import com.typesafe.config.{ConfigException, ConfigFactory, Config}
import models.JobReport
import org.apache.logging.log4j.LogManager
import play.api.Configuration
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import services.ContextManagerMessages.ContextInitError
import services.JobInfoManagerMessages.{GetJobStatus, GetJobStatuses}
import services.actors.JobManagerActorMessages.{JobLoadingError, StartJob, KillJob}
import services.io.JobInfo
import services.protocals.CommonMessages._
import services.util.SparkJobUtils
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.pattern.ask
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.Try

/**
  * Created by king on 15-11-18.
  */
@Singleton
class JobInfoController @Inject() (@Named("jobinfo-actor") jobInfo: ActorRef,
                                   @Named("context-actor") contextManager: ActorRef,
                                   configuration: Configuration) extends Controller {
  implicit val timeout = Timeout(5.seconds)
  private val logger = LogManager.getLogger(getClass)
  val config = configuration.underlying
  val StatusKey = "status"
  val ResultKey = "result"
  val contextTimeout = SparkJobUtils.getContextTimeout(config)
  val DefaultSyncTimeout = Timeout(10.seconds)

  val errorEvents: Set[Class[_]] = Set(classOf[JobErroredOut], classOf[JobValidationFailed])
  val asyncEvents = Set(classOf[JobStarted]) ++ errorEvents
  val syncEvents = Set(classOf[JobResult]) ++ errorEvents

  logger.info("JobInfoController is initialized.")

  /**
    * GET /jobs   -- returns a JSON list of hashes containing job status, ex:
    * [
    *   {jobId: "word-count-2013-04-22", status: "RUNNING"}
    * ]
    * optional @param limit Int - optional limit to number of jobs to display, defaults to 50
    */
  def listJobs(limit: Int) = Action.async {
    (jobInfo ? GetJobStatuses(Some(limit))).mapTo[Seq[JobInfo]].map(infos =>
      Ok(Json.toJson(infos.map(JobReport(_))))
    )
  }

  // GET /jobs/<jobId>
  // Returns job information in JSON.
  // If the job isn't finished yet, then {"status": "RUNNING" | "ERROR"} is returned.
  // Returned JSON contains result attribute if status is "FINISHED"
  def getJob(jobId: String) = Action.async {
    (jobInfo ? GetJobResult(jobId)).map{
      case NoSuchJobId =>
        NotFound(s"No such job id $jobId")
      case info: JobInfo =>
        Ok(Json.toJson(JobReport(info)))
      case JobResult(_, result) =>
        Ok(Json.toJson(Map("jobid" -> jobId, "result" -> result.toString)))
    }
  }

  /**
    * DELETE /jobs/<jobId>
    * Stop the current job. All other jobs submited with this spark context
    * will continue to run
    */
  def cancelJob(jobId: String) = Action.async {
    (jobInfo ? GetJobStatus(jobId)).map {
      case NoSuchJobId =>
        NotFound(s"No such job ID $jobId")
      case JobInfo(_, contextName, _, classPath, _, _, None, _) =>
        val jobManager = getJobManagerForContext(Some(contextName), config, classPath)
        jobManager.get ! KillJob(jobId)
        Ok(s"Job $jobId KILLED")
      case JobInfo(_, _, _, _, _, _, _, Some(ex)) =>
        Ok(s"Job error $ex")
      case JobInfo(_, _, _, _, _, _, Some(e), None) =>
        NotFound(s"No running job with ID $jobId")
    }
  }

  /**
    * POST /jobs   -- Starts a new job.  The job JAR must have been previously uploaded, and
    *                 the classpath must refer to an object that implements SparkJob.  The `validate()`
    *                 API will be invoked before `runJob`.
    *
    * entity         The POST entity should be a Typesafe Config format file;
    *                 It will be merged with the job server's config file at startup.
    * required @param appName String - the appName for the job JAR
    * required @param classPath String - the fully qualified class path for the job
    * optional @param context String - the name of the context to run the job under.  If not specified,
    *                                   then a temporary context is allocated for the job
    * optional @param sync Boolean if "true", then wait for and return results, otherwise return job Id
    * optional @param timeout Int - the number of seconds to wait for sync results to come back
    * return JSON result of { StatusKey -> "OK" | "ERROR", ResultKey -> "result"}, where "result" is
    *         either the job id, or a result
    */
  import models.SubmitJobForm._
  def submitJob() = Action.async(parse.form(submitJobForm)){ implicit request =>
    val submitJobData = request.body
    val appName = submitJobData.appName
    val classPath = submitJobData.classPath
    val contextOpt = submitJobData.contextOpt
    logger.info(s"submitJobData = $submitJobData")

    try {
      val async = !submitJobData.syncOpt.getOrElse(false)
      val jobConfig = ConfigFactory.parseString(submitJobData.config).withFallback(config.getConfig("spark")).resolve()
      val contextConfig = Try(jobConfig.getConfig("context-settings")).getOrElse(ConfigFactory.empty())
//      logger.info("context config " + contextConfig.root().render())
      val jobManager = getJobManagerForContext(contextOpt, contextConfig, classPath)
      val events = if (async) asyncEvents else syncEvents
      val timeout = submitJobData.timeoutOpt.map(t => Timeout(t.seconds)).getOrElse(DefaultSyncTimeout)
      (jobManager.get ? StartJob(appName, classPath, jobConfig, events))(timeout).map {
        // from jobResultActor
        case JobResult(jobId, res) => Ok(Json.toJson(Map("jobid" -> jobId, "result" -> res.toString)))
        // from jobStatusActor
        case JobErroredOut(jobId, _, ex) =>
          InternalServerError(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> s"job $jobId error out by $ex")))
        case JobStarted(jobId, context, _) =>
          Accepted(Json.toJson(Map(StatusKey -> "STARTED", "jobId" -> jobId, "context" -> context)))
        case JobValidationFailed(_, _, ex) =>
          BadRequest(Json.toJson(Map(StatusKey -> "VALIDATION FAILED", ResultKey -> s"$ex")))

        // from jobManagerActor
        case NoSuchApplication =>
          NotFound(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> s"appName $appName not found")))
        case NoSuchClass =>
          NotFound(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> s"classPath $classPath not found")))
        case JobLoadingError(err) =>
          InternalServerError(Json.toJson(Map(StatusKey -> "JOB LOADING FAILED", ResultKey -> s"$err")))
        case WrongJobType =>
          BadRequest(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> "Invalid job type for this context")))
        case NoJobSlotsAvailable(maxJobSlots) =>
          val errorMsg = "Too many running jobs (" + maxJobSlots +
            ") for job context '" + contextOpt.getOrElse("ad-hoc") + "'"
          ServiceUnavailable(Json.toJson(Map(StatusKey -> "NO SLOTS AVAILABLE", ResultKey -> errorMsg)))
        // unknown in the source code
        case ContextInitError(e) =>
          InternalServerError(Json.toJson(Map(StatusKey -> "CONTEXT INIT FAILED", ResultKey -> s"$e")))
      } recover {
        case e: Exception =>
          InternalServerError(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> s"$e")))
      }
    } catch {
      case e: NoSuchElementException =>
        Future(NotFound(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> s"context ${contextOpt.get} not found"))))
      case e: ConfigException =>
        Future(BadRequest(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> s"Cannot parse config: ${e.getMessage}"))))
      case e: Exception =>
        Future(InternalServerError(Json.toJson(Map(StatusKey -> "ERROR", ResultKey -> s"$e"))))
    }
  }


  private def getJobManagerForContext(context: Option[String],
                                      contextConfig: Config,
                                      classPath: String): Option[ActorRef] = {
    import services.ContextManagerMessages._
    val msg =
      if (context.isDefined) {
        GetContext(context.get)
      } else {
        GetAdHocContext(classPath, contextConfig)
      }
    val future = (contextManager ? msg)(contextTimeout.seconds)
    Await.result(future, contextTimeout.seconds) match {
      case (manager: ActorRef, resultActor: ActorRef) => Some(manager)
      case NoSuchContext                              => None
      case ContextInitError(err)                      => throw new RuntimeException(err)
    }
  }

//  private def getJobReport(jobInfo: JobInfo): Map[String, String] = {
//    Map("jobId" -> jobInfo.jobId,
//      "startTime" -> jobInfo.startTime.toString,
//      "classPath" -> jobInfo.classPath,
//      "config" -> jobInfo.config.root().render(),
//      "context" -> (if (jobInfo.contextName.isEmpty) "<<ad-hoc>>" else jobInfo.contextName),
//      "duration" -> jobInfo.jobLengthMillis.map { ms => ms / 1000.0 + " secs" }
//        .getOrElse("Job not done yet")) ++
//      (jobInfo match {
//      case JobInfo(_, _, _, _, _, _, None, _) => Map(StatusKey -> "RUNNING")
//      case JobInfo(_, _, _, _, _, _, _, Some(ex)) => Map(StatusKey -> "ERROR",
//        ResultKey -> ex.toString)
//      case JobInfo(_, _, _, _, _, _, Some(e), None) => Map(StatusKey -> "FINISHED")
//    })
//  }


}
