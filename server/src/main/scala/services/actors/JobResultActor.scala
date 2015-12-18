package services.actors

import akka.actor.{ActorRef, Actor}
import org.apache.logging.log4j.LogManager
import services.util.LRUCache

import scala.collection.mutable

/**
  * Created by king on 15-11-17.
  */
class JobResultActor extends Actor {
  import services.protocals.CommonMessages._
  private val logger = LogManager.getLogger(getClass)
  private val config = context.system.settings.config
  private val cache = new LRUCache[String, Any](config.getInt("spark.server.job-result-cache-size"))
  private val subscribers = mutable.HashMap.empty[String, ActorRef]

  override def receive: Receive = {
    case Subscribe(jobId, receiver, events) =>
      if (events.contains(classOf[JobResult])) {
        subscribers(jobId) = receiver
        logger.info(s"Added receiver $receiver to subscriber list for JobID $jobId")
      }

    case Unsubscribe(jobId, receiver) =>
      if (!subscribers.contains(jobId)) {
        sender ! NoSuchJobId
      } else {
        subscribers.remove(jobId)
        logger.info(s"Removed subscriber list for JobID $jobId")
      }

    case GetJobResult(jobId) =>
      sender ! cache.get(jobId).map(JobResult(jobId, _)).getOrElse(NoSuchJobId)

    case JobResult(jobId, result) =>
      cache.put(jobId, result)
      logger.info(s"Received job results for JobID $jobId")
      subscribers.get(jobId).foreach(_ ! JobResult(jobId, result))
      subscribers.remove(jobId)
  }

}
