package services.protocals

import java.util.Date

import akka.actor.ActorRef
import services.io.JobInfo

/**
 * Created by king on 15-9-21.
 */


trait StatusMessage {
  val jobId: String
}

// Messages that are sent and received by multiple actors.
object CommonMessages {
  // job status messages
  case class JobStarted(jobId: String, context: String, startTime: Date) extends StatusMessage
  case class JobFinished(jobId: String, endTime: Date) extends StatusMessage
  case class JobValidationFailed(jobId: String, endTime: Date, err: Throwable) extends StatusMessage
  case class JobErroredOut(jobId: String, endTime: Date, err: Throwable) extends StatusMessage
  case class JobKilled(jobId: String, endTime: Date) extends StatusMessage

  case class JobInit(jobInfo: JobInfo)
  case class GetRunningJobStatus()

  /**
   * NOTE: For Subscribe, make sure to use `classOf[]` to get the Class for the case classes above.
   * Otherwise, `.getClass` will get the `java.lang.Class` of the companion object.
   */
  case class GetJobResult(jobId: String)
  case class JobResult(jobId: String, result: Any)

  case class Subscribe(jobId: String, receiver: ActorRef, events: Set[Class[_]]) {
    require(events.nonEmpty, "Must subscribe to at least one type of event!")
  }
  case class Unsubscribe(jobId: String, receiver: ActorRef) // all events for this jobId and receiving actor

  // errors
  case object NoSuchJobId
  case object NoSuchApplication
  case object NoSuchClass
  case object WrongJobType      // Job type C does not match context type
  case object JobInitAlready
  case class NoJobSlotsAvailable(maxJobSlots: Int) // TODO: maybe rename this
}
