package services.util

import cn.edu.zju.king.serverapi.SharedRdds
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by king on 15-12-7.
  */
class JobServerSharedRdds(sc: SparkContext) extends SharedRdds {

  /**
    * a hashMap, name -> rdd.id
    * SparkContext.getPersistentRDDs.get(rdd.id)
    */
  private val namesToRdds = mutable.HashMap[String, Int]().empty

  override def get[T](name: String): Option[RDD[T]] = ???

  override def update[T](name: String, rddGen: => RDD[T]): RDD[T] = ???

  override def names: Iterable[String] = ???

  override def destroy(name: String): Unit = ???
}
