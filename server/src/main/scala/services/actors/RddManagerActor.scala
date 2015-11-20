package services.actors

import akka.actor.{ActorRef, Actor}
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable

/**
  * Created by king on 15-11-17.
  */
object RddManagerActorMessages {
  // Message which asks to retrieve an RDD by name. If no such RDD is found, None will be returned.
  case class GetRddRequest(name: String)

  // Message which asks to retrieve an RDD by name. Different from GetRddRequest, because it tells the
  // RddManager that the client is willing to create the RDD with this name if one does not already exist.
  case class GetOrElseCreateRddRequest(name: String)

  // Message which tells the RddManager that a new RDD has been created, or that RDD generation failed.
  case class CreateRddResult(name: String, rddOrError: Either[Throwable, RDD[_]])

  // Message which tells the RddManager that an RDD should be destroyed and all of its cached blocks removed
  case class DestroyRdd(name: String)

  // Message which asks for the names of all RDDs currently managed by the RddManager
  case object GetRddNames
}

class RddManagerActor(sparkContext: SparkContext) extends Actor {
  import RddManagerActorMessages._
  private val logger = LogManager.getLogger(getClass)

  private val namesToRDDs = new mutable.HashMap[String, RDD[_]]()
  private val waiters = new mutable.HashMap[String, mutable.Set[ActorRef]] with mutable.MultiMap[String, ActorRef]
  private val inProgress = mutable.Set[String]()

  override def receive: Receive = {
    case GetRddRequest(name) =>
      sender ! getExistingRdd(name)

    case GetOrElseCreateRddRequest(name) if inProgress.contains(name) =>
      logger.info(s"RDD [$name] already being created, actor ${sender().path} added to waiters list")
      waiters.addBinding(name, sender())

    case GetOrElseCreateRddRequest(name) => getExistingRdd(name) match {
      case Some(rdd) => sender ! Right(rdd)
      case None =>
        logger.info(s"RDD [$name] not found, starting creation")
        inProgress.add(name)
        sender ! None
    }

    case CreateRddResult(name, Left(error)) => notifyAndClearWaiters(name, Left(error))

    case CreateRddResult(name, Right(rdd)) =>
      val oldRddOption = getExistingRdd(name)
      namesToRDDs(name) = rdd
      notifyAndClearWaiters(name, Right(rdd))
      if (oldRddOption.isDefined && oldRddOption.get.id != rdd.id) {
        oldRddOption.get.unpersist(blocking = false)
      }

    case DestroyRdd(name) => getExistingRdd(name).foreach { rdd =>
      namesToRDDs.remove(name)
      rdd.unpersist(blocking = false)
    }

    case GetRddNames =>
      val persistentRdds = sparkContext.getPersistentRDDs
      val result = namesToRDDs.collect{ case (name, rdd) if persistentRdds.contains(rdd.id) => name }
      sender ! result
  }

  private def getExistingRdd(name: String): Option[RDD[_]] = {
    namesToRDDs.get(name).flatMap{ rdd =>
      sparkContext.getPersistentRDDs.get(rdd.id)
    } match {
      case Some(rdd) => Some(rdd)
      case None =>
        namesToRDDs.remove(name)
        None
    }
  }

  private def notifyAndClearWaiters(name: String, message: Any): Unit = {
    waiters.get(name).foreach { actors => actors.foreach { actor => actor ! message } }
    waiters.remove(name) // Note: this removes all bindings for the key in the MultiMap
    inProgress.remove(name) // this RDD is no longer being computed, clear in progress flag
  }


}