package services.io

import java.io._
import java.nio.file.{Files, Paths}
import java.util.Date
import javax.inject.Inject

import com.google.inject.Singleton
import com.typesafe.config.{ConfigRenderOptions, ConfigFactory}
import org.apache.logging.log4j.LogManager
import play.api.Configuration

import scala.collection.mutable

/**
  * Created by king on 15-11-16.
  */
@Singleton
class JobFileDAO @Inject() (config: Configuration) extends JobDAO {
  private val logger = LogManager.getLogger(getClass)

  private val apps = mutable.HashMap.empty[String, Seq[Date]]
  private val jobs = mutable.HashMap.empty[String, JobInfo]

  private val rootDirPath = Paths.get(config.getString("spark.jobserver.filedao.rootdir")
    .getOrElse("/tmp/spark-server"))
  logger.info("root dir is " + rootDirPath.toAbsolutePath)

  private val jarsFilePath = rootDirPath.resolve("jars.data")
  private var jarsOutputStream: DataOutputStream = null
  private val jobsFilePath = rootDirPath.resolve("jobs.data")
  private var jobsOutputStream: DataOutputStream = null

  // load data from files
  init()

  private def init(): Unit = {
    // create the date directory if it doesn't exist
    Files.createDirectories(rootDirPath)
    if (Files.notExists(rootDirPath)) {
      throw new RuntimeException("Could not create directory " + rootDirPath.toAbsolutePath)
    }

    // read all jar info from file
    if (Files.exists(jarsFilePath)) {
      val in = new DataInputStream(new BufferedInputStream(Files.newInputStream(jarsFilePath)))
      try {
        while (in.available() != 0) {
          val jarInfo = readJarInfo(in)
          addJar(jarInfo.appName, jarInfo.uploadTime)
        }
      } catch {
        case e: Exception => logger.warn("Could not read jar file by " + e)
      } finally {
        in.close()
      }
    }

    // read all app info from file
    if (Files.exists(jobsFilePath)) {
      val in = new DataInputStream(new BufferedInputStream(Files.newInputStream(jobsFilePath)))
      try {
        while (in.available() != 0) {
          val jobInfo = readJobInfo(in)
          jobs(jobInfo.jobId) = jobInfo
        }
      } catch {
        case e: EOFException => logger.warn("Read EOF when reading job file " + jobsFilePath.toAbsolutePath)
        case e: Exception => throw e
      } finally {
        in.close()
      }
    }

    // init file output stream
    jarsOutputStream = new DataOutputStream(new FileOutputStream(jarsFilePath.toFile, true))
    jobsOutputStream = new DataOutputStream(new FileOutputStream(jobsFilePath.toFile, true))
  }

  private def writeJarInfo(out: DataOutputStream, jarInfo: JarInfo) {
    out.writeUTF(jarInfo.appName)
    out.writeLong(jarInfo.uploadTime.getTime)
  }

  private def readJarInfo(in: DataInputStream): JarInfo = JarInfo(in.readUTF(), new Date(in.readLong()))

  private def addJar(appName: String, uploadTime: Date): Unit = {
    if (apps.contains(appName)) {
      apps(appName) = uploadTime +: apps(appName)
    } else {
      apps(appName) = Seq(uploadTime)
    }
  }

  private def writeJobInfo(out: DataOutputStream, jobInfo: JobInfo) {
    out.writeUTF(jobInfo.jobId)
    // write context name
    out.writeUTF(jobInfo.contextName)
    // write jar info
    writeJarInfo(out, jobInfo.jarInfo)
    // write classpath
    out.writeUTF(jobInfo.classPath)
    // write config
    out.writeUTF(jobInfo.config.root().render(ConfigRenderOptions.concise()))
    // write start time
    out.writeLong(jobInfo.startTime.getTime)
    // write end time
    val time = if (jobInfo.endTime.isEmpty) jobInfo.startTime.getTime else jobInfo.endTime.get.getTime
    out.writeLong(time)
    val errorStr = if (jobInfo.error.isEmpty) "" else jobInfo.error.get.toString
    out.writeUTF(errorStr)
  }

  private def readJobInfo(in: DataInputStream): JobInfo = {
    JobInfo(in.readUTF(), in.readUTF(),
      JarInfo(in.readUTF(), new Date(in.readLong())),
      in.readUTF(), ConfigFactory.parseString(in.readUTF()),
      new Date(in.readLong()), Some(new Date(in.readLong())), readError(in))
  }

  private def readError(in: DataInputStream) = {
    val error = in.readUTF()
    if (error == "") None else Some(new Throwable(error))
  }

  /**
    * Persist a jar.
    */
  override def saveJar(appName: String, uploadTime: Date, jarBytes: Array[Byte]): Unit = {
    // store jar file
    val outJarFilePath = rootDirPath.resolve(appName + "-" + uploadTime.getTime + ".jar")
    val bos = new BufferedOutputStream(new FileOutputStream(outJarFilePath.toFile))
    try {
      logger.info("writing " + jarBytes.length + " bytes to file {}", outJarFilePath.toAbsolutePath.toString)
      bos.write(jarBytes)
      bos.flush()
    } finally {
      bos.close()
    }

    // write jarinfo into jarsinfofile
    writeJarInfo(jarsOutputStream, JarInfo(appName, uploadTime))

    // register the jarinfo
    addJar(appName, uploadTime)
  }

  /**
    * Return all applications name and their last upload times.
    */
  override def getApps: Map[String, Date] = apps.map(m => (m._1, m._2.head)).toMap

  /**
    * get the jar file path, for sparkcontext addjar
    */
  override def retrieveJarFile(appName: String, uploadTime: Date): String =
    rootDirPath.resolve(appName + "-" + uploadTime.getTime + ".jar").toAbsolutePath.toString

  /**
    * Return all job ids to their job info.
    */
  override def getJobInfos(limit: Int): Seq[JobInfo] = jobs.values.toSeq.sortBy(- _.startTime.getTime).take(limit)

  /**
    * Return job info for a specific job id.
    */
  override def getJobInfo(jobId: String): Option[JobInfo] = jobs.get(jobId)

  /**
    * Persist a job info.
    */
  override def saveJobInfo(jobInfo: JobInfo): Unit = {
    writeJobInfo(jobsOutputStream, jobInfo)
    // register the job info
    jobs(jobInfo.jobId) = jobInfo
  }


}
