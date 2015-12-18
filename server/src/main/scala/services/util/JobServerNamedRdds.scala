package services.util

import akka.actor.ActorRef
import akka.util.Timeout
import cn.edu.zju.king.serverapi.NamedRdds
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import services.actors.RddManagerActor._

import scala.concurrent.Await

/**
 * Created by king on 15-9-22.
 */
class JobServerNamedRdds(val rddManager: ActorRef) extends NamedRdds {
  require(rddManager != null, "rddManager ActorRef must not be null!")


  override def getOrElseCreate[T](name: String, rddGen: => RDD[T])
                                 (implicit timeout: Timeout = defaultTimeout): RDD[T] = {
    import akka.pattern.ask
    val future = rddManager.ask(GetOrElseCreateRddRequest(name))
    val result: RDD[T] = Await.result(future, timeout.duration) match {
      case Left(error: Throwable) =>
        throw new RuntimeException("Failed to get named RDD '" + name + "'", error)
      case Right(rdd: RDD[T]) => refreshRdd(rdd)
      case None =>
        // Try to generate the RDD and send the result of the operation to the rddManager.
        try {
          val rdd = createRdd(rddGen, name)
          rddManager ! CreateRddResult(name, Right(rdd))
          rdd
        } catch {
          case error: Throwable =>
            rddManager ! CreateRddResult(name, Left(error))
            throw new RuntimeException("Failed to create named RDD '" + name + "'", error)
        }
    }
    result
  }

  override def update[T](name: String, rddGen: => RDD[T]): RDD[T] = {
    val rdd = createRdd(rddGen, name)
    rddManager ! CreateRddResult(name, Right(rdd))
    rdd
  }

  override def get[T](name: String)
                     (implicit timeout: Timeout = defaultTimeout): Option[RDD[T]] = {
    import akka.pattern.ask

    val future = rddManager ? GetRddRequest(name)
    Await.result(future, timeout.duration) match {
      case rddOpt: Option[RDD[T]] @unchecked => rddOpt.map { rdd => refreshRdd(rdd) }
    }
  }

  override def destroy(name: String): Unit = {
    rddManager ! DestroyRdd(name)
  }

  override def getNames()
                       (implicit timeout: Timeout = defaultTimeout): Iterable[String] = {
    import akka.pattern.ask

    val future = rddManager ? GetRddNames
    Await.result(future, timeout.duration) match {
      case answer: Iterable[String] @unchecked => answer
    }
  }


  private def createRdd[T](rddGen: => RDD[T],
                           name: String,
                           forceComputation: Boolean = true,
                           storageLevel: StorageLevel = defaultStorageLevel): RDD[T] = {
    require(!forceComputation || storageLevel != StorageLevel.NONE,
      "forceComputation implies storageLevel != NONE")
    val rdd = rddGen
    rdd.setName(name)
    rdd.getStorageLevel match {
      case StorageLevel.NONE => rdd.persist(storageLevel)
      case currentLevel => rdd.persist(currentLevel)
    }
    if (forceComputation) rdd.count()
    rdd
  }

  private def refreshRdd[T](rdd: RDD[T]): RDD[T] = rdd.persist(rdd.getStorageLevel)
}
