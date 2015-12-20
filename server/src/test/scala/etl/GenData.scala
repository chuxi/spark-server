package etl

import java.io.{InputStreamReader, BufferedReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, AbstractFileSystem}

import scala.io.Source

/**
  * Program:  used for test data generation.
  *           get a sample.txt on HDFS, which contains following data:
  *
  * schema:
  *   number, name, gender, age, math, physics, chemistry
  *
  * data sample:
  *   10001, sub-1, male, 23, 93, 90, 97
  *   10002, sub-2, female, 18, 64, 66, 67
  *   10003, sub-3, male, 23, 66, 78, 83
  *   ...
  *
  */
object GenData {
  val hdfs = "hdfs://192.168.1.11:9000"
  val fp = "hdfs:/etl/sample.txt"
  val NUM = 20000

  def main(args: Array[String]) {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfs)
    val fs = FileSystem.get(conf)
    val file = new Path(fp)
    val out = fs.create(file)
    def randAge = math.round(math.random * 6 + 18)
    // get a score between 60~100
    def randScore = math.round(math.random * 40 + 60)

    var t = 1
    while (t < NUM) {
      val start = t
      t = t + 10000
      start to t map{ i =>
        s"${10000 + i}, ${"sub-" + i}, ${if (i%2 == 0) "female" else "male"}, $randAge, $randScore, $randScore, $randScore\n"
      } foreach out.writeBytes
      out.flush()
    }

    out.close()

    val in = fs.open(file)
    val br = Source.fromInputStream(in)
    br.getLines().foreach(println)
    br.close()

    fs.close()
  }

}
