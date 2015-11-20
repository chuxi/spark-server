package services.actors

import akka.actor.{ActorRef, Actor}
import org.apache.logging.log4j.LogManager
import services.io.{JobInfo, JobDAO}
import services.protocals.{StatusMessage, CommonMessages}
import scala.collection.mutable

/**
  * Created by king on 15-11-17.
  */
class JobStatusActor(jobDao: JobDAO) extends Actor {
  import CommonMessages._
  private val logger = LogManager.getLogger(getClass)

  // job id to jobinfo
  private val infos = new mutable.HashMap[String, JobInfo]()
  // subscribes
  private val subscribes = new mutable.HashMap[String, mutable.MultiMap[Class[_], ActorRef]]()

  override def receive: Receive = {
    case GetRunningJobStatus =>
      sender ! infos.values.toSeq.sortBy(_.startTime)

    case Unsubscribe(jobId, receiver) =>
      subscribes.get(jobId) match {
        case Some(jobSubscribers) =>
          jobSubscribers.transform{case (event, receivers) => receivers -= receiver}
            .retain{case (event, receivers) => receivers.nonEmpty}
          if (jobSubscribers.isEmpty) subscribes.remove(jobId)

        case None =>
          logger.error("No such job id " + jobId)
          sender ! NoSuchJobId
      }

    case Subscribe(jobId, receiver, events) =>
      val jobSubscribers = subscribes.getOrElseUpdate(jobId, newMultiMap())
      events.foreach{ event => jobSubscribers.addBinding(event, receiver)}

    case JobInit(jobInfo) =>
      if (!infos.contains(jobInfo.jobId)) {
        infos(jobInfo.jobId) = jobInfo
      } else {
        sender ! JobInitAlready
      }

    case msg: JobStarted =>
      processStatus(msg, "started") {
        case (info, m) =>
          info.copy(startTime = m.startTime)
      }

    case msg: JobFinished =>
      processStatus(msg, "finished OK", remove = true) {
        case (info, m) =>
          info.copy(endTime = Some(m.endTime))
      }

    case msg: JobValidationFailed =>
      processStatus(msg, "validation failed", remove = true) {
        case (info, m) =>
          info.copy(endTime = Some(m.endTime), error = Some(m.err))
      }

    case msg: JobErroredOut =>
      processStatus(msg, "finished with an error", remove = true) {
        case (info, m) =>
          info.copy(endTime = Some(m.endTime), error = Some(m.err))
      }

    case msg: JobKilled =>
      processStatus(msg, "killed", remove = true) {
        case (info, m) =>
          info.copy(endTime = Some(m.endTime))
      }
  }

  private def processStatus[M <: StatusMessage](msg: M, logMessage: String, remove: Boolean = false)
                                               (infoModifier: (JobInfo, M) => JobInfo): Unit = {
    if (infos.contains(msg.jobId)) {
      infos(msg.jobId) = infoModifier(infos(msg.jobId), msg)
      logger.info(s"Job $msg.jobId $logMessage")
      jobDao.saveJobInfo(infos(msg.jobId))
      publishMessage(msg.jobId, msg)
      if (remove) infos.remove(msg.jobId)
    } else {
      logger.error("No such job id " + msg.jobId)
      sender ! NoSuchJobId
    }
  }

  private def publishMessage(jobId: String, message: StatusMessage): Unit = {
    for (
      jobSubscribers <- subscribes.get(jobId);
      receivers <- jobSubscribers.get(message.getClass);
      receiver <- receivers) {
      receiver ! message
    }
  }

  private def newMultiMap(): mutable.MultiMap[Class[_], ActorRef] =
    new mutable.HashMap[Class[_], mutable.Set[ActorRef]] with mutable.MultiMap[Class[_], ActorRef]
}
