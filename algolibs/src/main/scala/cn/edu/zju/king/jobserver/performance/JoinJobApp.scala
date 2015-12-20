package cn.edu.zju.king.jobserver.performance

import cn.edu.zju.king.serverapi.{SparkJobInvalid, SparkJobValid, SparkJobValidation, SparkJob}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

/**
  * Created by king on 15-12-18.
  */
class JoinJobApp extends SparkJob {
  val master = "10.214.208.11"
  val file1 = s"hdfs://$master:9000/etl/bigsample-1.txt"
  val file2 = s"hdfs://$master:9000/etl/bigsample-2.txt"
  val schema1 = "number, name, gender, age, math, physics, chemistry"
  val schema2 = "number, name, english, chinese, biology"
  val joinsql =
    """
      |select t1.number as number, t1.name, t1.gender, t1.age,
      |t1.math, t1.physics, t1.chemistry, t2.english, t2.chinese, t2.biology
      |from table1 t1 join table2 t2 on (t1.name = t2.name)
    """.stripMargin

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)
    val parts = jobConfig.getInt("partitions")
    val data1 = sc.textFile(file1, parts).map(_.split(",").map(_.trim))
    val data2 = sc.textFile(file2, parts).map(_.split(",").map(_.trim))

    // to make the data load into memory
//    data1.count()
//    data2.count()

    val t1 = System.currentTimeMillis()

    def createTable(data: RDD[Array[String]], tableName: String, schemaString: String): Unit = {
      // get schema
      val schema = StructType(schemaString.split(",")
        .map(fieldName => StructField(fieldName.trim, StringType, true)))
      if (schema.length != data.first().length)
        throw new Exception("schema is not corresponding to the RDD columns")
      val rowRdd = data.map(m => Row.fromSeq(m))
      val df = sqlContext.createDataFrame(rowRdd, schema)
      df.registerTempTable(tableName)
    }

    createTable(data1, "table1", schema1)
    createTable(data2, "table2", schema2)

    val result = sqlContext.sql(joinsql)
    // make the action and get sample data to make sure it runs correctly
    result.sort("number").take(10).foreach(println)

    val t2 = System.currentTimeMillis()
    t2 - t1
  }

  override def validate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {
    Try(jobConfig.getInt("partitions")).map(x => SparkJobValid).getOrElse(SparkJobInvalid("missing partitions in jobConfig"))
  }

}
