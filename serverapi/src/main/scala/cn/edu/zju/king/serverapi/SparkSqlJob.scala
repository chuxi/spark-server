package cn.edu.zju.king.serverapi

import org.apache.spark.sql.SQLContext

/**
 * Created by king on 15-10-20.
 */
trait SparkSqlJob extends SparkJobBase {
  type C = SQLContext
}