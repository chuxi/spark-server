package services

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import com.google.inject.name.Named
import com.google.inject.{Inject, Singleton}
import org.apache.logging.log4j.LogManager
import services.ContextManager.GetResultActor

import services.io.JobDAO
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by king on 15-11-18.
  */

object JobInfoManager {
  // Requests
  case class GetJobStatuses(limit: Option[Int])
//  case class GetJobConfig(jobId: String)
  case class GetJobStatus(jobId: String)
//  case class StoreJobConfig(jobId: String, jobConfig: Config)

  // Responses
  case object JobConfigStored
}

@Singleton
class JobInfoManager @Inject() (jobDao: JobDAO, @Named("context-actor") contextManager: ActorRef) extends Actor {
  import services.JobInfoManager._
  import services.protocals.CommonMessages._
  private val logger = LogManager.getLogger(getClass)
  implicit val ShortTimeout = Timeout(3.seconds)

  logger.info("JobInfo Manager Actor started successfully!")

  override def receive: Receive = {
    case GetJobStatuses(limit) =>
      sender ! jobDao.getJobInfos(limit.get)

    case GetJobStatus(jobId) =>
      val jobInfo = jobDao.getJobInfo(jobId)
      val resp = if (jobInfo.isEmpty) NoSuchJobId else jobInfo.get
      sender ! resp

    case GetJobResult(jobId) =>
      scala.util.control.Breaks.breakable {
        val jobInfo = jobDao.getJobInfo(jobId)

        if (jobInfo.isEmpty) {
          sender ! NoSuchJobId
          scala.util.control.Breaks.break()
        }

        jobInfo.filter { job => job.isRunning || job.isErroredOut }
          .foreach { jobInfo =>
            sender ! jobInfo
            scala.util.control.Breaks.break()
          }

        // get the context from jobInfo
        val context = jobInfo.get.contextName

        val future = (contextManager ? GetResultActor(context)).mapTo[ActorRef]
        val resultActor = Await.result(future, 3.seconds)

        val receiver = sender() // must capture the sender since callbacks are run in a different thread
        for (result <- resultActor ? GetJobResult(jobId)) {
          receiver ! result // a JobResult(jobId, result) object is sent
        }
      }

//    case GetJobConfig(jobId) =>
//      sender ! jobDao.getJobConfigs.getOrElse(jobId, NoSuchJobId)
//
//    case StoreJobConfig(jobId, jobConfig) =>
//      jobDao.saveJobConfig(jobId, jobConfig)
//      sender ! JobConfigStored
  }
}
