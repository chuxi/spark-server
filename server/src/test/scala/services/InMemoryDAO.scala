package services

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.Date

import services.io.{JobDAO, JobInfo}

import scala.collection.mutable

/**
 * Created by king on 15-10-9.
 * In-memory DAO for easy unit testing
 */

class InMemoryDAO extends JobDAO {
  val jars = mutable.HashMap.empty[(String, Date), Array[Byte]]

  override def saveJar(appName: String, uploadTime: Date, jarBytes: Array[Byte]): Unit = jars((appName, uploadTime)) = jarBytes

  override def getApps: Map[String, Date] = {
    jars.keys
      .groupBy(_._1)
      .map { case (appName, appUploadTimeTuples) =>
      appName -> appUploadTimeTuples.map(_._2).toSeq.head
    }
  }

  override def retrieveJarFile(appName: String, uploadTime: Date): String = {
    // Write the jar bytes to a temporary file
    val outFile = java.io.File.createTempFile("InMemoryDAO", ".jar")
    outFile.deleteOnExit()
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      bos.write(jars((appName, uploadTime)))
    } finally {
      bos.close()
    }
    outFile.getAbsolutePath()
  }

  val jobInfos = mutable.HashMap.empty[String, JobInfo]

  override def getJobInfos(limit: Int = 50): Seq[JobInfo] = jobInfos.values.toSeq.sortBy(_.startTime.toString).take(limit)

  override def getJobInfo(jobId: String): Option[JobInfo] = jobInfos.get(jobId)

  override def saveJobInfo(jobInfo: JobInfo): Unit = jobInfos(jobInfo.jobId) = jobInfo

  def clean(): Unit = {
    jars.clear()
    jobInfos.clear()
  }
}
