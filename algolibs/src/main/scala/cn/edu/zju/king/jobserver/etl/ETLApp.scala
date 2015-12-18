package cn.edu.zju.king.jobserver.etl

import cn.edu.zju.king.serverapi._
import com.typesafe.config.{ConfigRenderOptions, Config}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, StringType, StructField}

import scala.util.Try

/**
  * Created by king on 15-12-14.
  *
  * sharedrdds: "source"
  *
  * etl {
  *   extract {
  *     table: "temptable"
  *     schema: "col1, col2, col3..."
  *     sql: "select ... from temptable where ..."
  *   }
  *
  *   transform {}
  *
  *   load {
  *     type: "file"
  *     url: "hdfs:///result.parquet"
  *   }
  * }
  */
class ETLApp extends SparkJob with NamedRddSupport {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)
    val data = namedRdds.get[Array[String]](jobConfig.getString("sharedrdds")).get
    // get schema
    val schema = StructType(jobConfig.getString("etl.extract.schema").split(",")
      .map(fieldName => StructField(fieldName.trim, StringType, true)))
    if (schema.length != data.first().length)
      return new Exception("schema is not corresponding to the RDD columns")
    val rowRdd = data.map(m => Row.fromSeq(m))
    val df = sqlContext.createDataFrame(rowRdd, schema)
    df.registerTempTable(jobConfig.getString("etl.extract.table"))
    // do extract
    val result = sqlContext.sql(jobConfig.getString("etl.extract.sql"))

//    result.collect().foreach(println)
    // do transform
    // ...

    // do load
    result.write.mode("overwrite").save(jobConfig.getString("etl.load.url"))
  }

  override def validate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {
    val error = SparkJobInvalid("spark job config error " + jobConfig.root().render(ConfigRenderOptions.concise()))
    Try(jobConfig.getConfig("etl")).map(x => {
      x.getString("extract.sql")
      x.getString("extract.table")
      x.getString("load.url")
    }).map(x => SparkJobValid)
    .getOrElse(error) &&
    Try(jobConfig.getString("sharedrdds")).map(x => SparkJobValid)
      .getOrElse(error) &&
    Try(namedRdds.get(jobConfig.getString("sharedrdds"))).map(x => SparkJobValid)
      .getOrElse(error)
  }

}
