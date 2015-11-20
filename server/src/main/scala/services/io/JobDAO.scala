package services.io

import java.util.Date

import com.typesafe.config.Config

/**
  * Created by king on 15-11-16.
  */
// Uniquely identifies the jar used to run a job
case class JarInfo(appName: String, uploadTime: Date)

// Both a response and used to track job progress
// NOTE: if endTime is not None, then the job has finished.
// compared with original, I merged config info with job info
case class JobInfo(jobId: String, contextName: String,
                   jarInfo: JarInfo, classPath: String,
                   config: Config, startTime: Date,
                   endTime: Option[Date], error: Option[Throwable]) {
  def jobLengthMillis: Option[Long] = endTime.map { end => end.getTime - startTime.getTime }

  def isRunning: Boolean = endTime.isEmpty
  def isErroredOut: Boolean = endTime.isDefined && error.isDefined
}

/**
  * Core trait for data access objects for persisting data such as jars(applications) jobs, etc.
  */
trait JobDAO {
  /**---------------jars------------------*/

  /**
    * Persist a jar.
    */
  def saveJar(appName: String, uploadTime: Date, jarBytes: Array[Byte])

  /**
    * Return all applications name and their last upload times.
    */
  def getApps: Map[String, Date]

  /**
    * get the jar file path, for sparkcontext addjar
    */
  def retrieveJarFile(appName: String, uploadTime: Date): String


  /**---------------jobs------------------*/
  /**
    * Persist a job info.
    */
  def saveJobInfo(jobInfo: JobInfo)

  /**
    * Return job info for a specific job id.
    */
  def getJobInfo(jobId: String): Option[JobInfo]

  /**
    * Return all job ids to their job info.
    */
  def getJobInfos(limit: Int): Seq[JobInfo]
}
