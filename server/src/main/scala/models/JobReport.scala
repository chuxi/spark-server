package models

import java.util.Date

import com.typesafe.config.{ConfigRenderOptions, Config}
import play.api.libs.json.{Json, JsValue, Writes}
import services.io.JobInfo

/**
  * Created by king on 15-11-25.
  */

case class JobReport(jobId: String, startTime: Date,
                     classPath: String, contextName: String,
                     config: Config, duration: Long, status: String, result: Any)

object JobReport {

  def apply(job: JobInfo) = {
    new JobReport(job.jobId, job.startTime, job.classPath, job.contextName, job.config, job.jobLengthMillis.getOrElse(0),
      if (job.isRunning) "RUNNING"
      else if (job.isFinished) "FINISHED"
      else if (job.isErroredOut) "ERROR" else "",
      if (job.isErroredOut) job.error.get else "")
  }

  val renderOpts = ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false).setComments(false)


  implicit val jobReportWrites = new Writes[JobReport] {
    override def writes(o: JobReport): JsValue = Json.obj(
      "jobId" -> o.jobId,
      "startTime" -> o.startTime,
      "classPath" -> o.classPath,
      "contextName" -> o.contextName,
      "config" -> o.config.root().render(renderOpts),
      "duration" -> o.duration,
      "status" -> o.status,
      "result" -> o.result.toString
    )
  }
}
