package cn.edu.zju.king.serverapi

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created by king on 15-12-7.
  * define the methods used for RDD management
  * the class extended this trait is required to have a construct parameter SparkContext
  * for example:
  * {{{
  *   class JobServerSharedRdds(sc: SparkContext) extends SharedRdds {
  *     ...
  *   }
  * }}}
  */
trait SharedRdds {
  val defaultStorageLevel = StorageLevel.MEMORY_ONLY

  def get[T](name: String): Option[RDD[T]]

  def update[T](name: String, rddGen: => RDD[T]): RDD[T]

  def destroy(name: String): Unit

  def names: Iterable[String]
}

trait SharedRddSupport { self: SparkJob =>
  val sharedRddsPrivate: AtomicReference[SharedRdds] = new AtomicReference[SharedRdds](null)

  def sharedRdds: SharedRdds = sharedRddsPrivate.get() match {
    case null => throw new NullPointerException("sharedRdds value is null!")
    case rdds: SharedRdds => rdds
  }

}