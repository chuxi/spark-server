package performance

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Program:  used for test data generation.
  *           get a sample.txt on HDFS, which contains following data:
  *
  * file-1
  * schema-1:
  *   number, name, gender, age, math, physics, chemistry
  *
  * data sample:
  *   100000001, sub-1, male, 23, 93, 90, 97
  *   100000002, sub-2, female, 18, 64, 66, 67
  *   100000003, sub-3, male, 23, 66, 78, 83
  *   ...
  *
  * file-2
  * schema-2:
  *   number, name, english, chinese, biology
  *   100000001, sub-1, 93, 90, 97
  *   100000002, sub-2, 64, 66, 67
  *   100000003, sub-3, 66, 78, 83
  *
  * use two schema to do join(shuffle)
  */
object GenBigData {
  val hdfs = "hdfs://10.214.208.11:9000"
  val bigfp1 = "hdfs:/etl/bigsample-1.txt"
  val bigfp2 = "hdfs:/etl/bigsample-2.txt"
  val NUM = 150000000

  def main(args: Array[String]) {
    def randAge = math.round(math.random * 6 + 18)
    // get a score between 60~100
    def randScore = math.round(math.random * 40 + 60)

    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfs)
    val fs = FileSystem.get(conf)

    // generate file-1
    val file1 = new Path(bigfp1)
    val out1 = fs.create(file1)
    var t = 1
    while (t < NUM) {
      val start = t
      t = t + 10000
      start to t map{ i =>
        s"${100000000 + i}, ${"sub-" + i}, ${if (i%2 == 0) "female" else "male"}, $randAge, $randScore, $randScore, $randScore\n"
      } foreach out1.writeBytes
      out1.flush()
    }
    out1.close()

    // generate file-2
    val file2 = new Path(bigfp2)
    val out2 = fs.create(file2)
    t = 1
    while (t < NUM) {
      val start = t
      t = t + 10000
      start to t map{ i =>
        s"${100000000 + i}, ${"sub-" + i}, $randScore, $randScore, $randScore\n"
      } foreach out2.writeBytes
      out2.flush()
    }
    out2.close()

    fs.close()
  }
}
