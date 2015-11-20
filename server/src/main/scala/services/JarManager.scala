package services

import java.nio.file.{Files, Paths}
import java.util.Date

import akka.actor.Actor
import com.google.inject.{Inject, Singleton}
import org.apache.logging.log4j.LogManager
import services.io.JobDAO
import services.util.JarUtils

/**
  * Created by king on 15-11-16.
  */
object JarManagerMessages {
  // Messages to JarManager actor
  /** Message for storing a JAR for an application given the byte array of the JAR file */
  case class StoreJar(appName: String, jarBytes: Array[Byte])

  /** Message requesting a listing of the available JARs */
  case object ListJars

  /** Message for storing one or more local JARs based on the given map.
    * @param  localJars    Map where the key is the appName and the value is the local path to the JAR.
    */
  case class StoreLocalJars(localJars: Map[String, String])

  // Responses
  case object InvalidJar
  case object JarStored
}

@Singleton
class JarManager @Inject() (jobDao: JobDAO) extends Actor {
  import JarManagerMessages._
  private val logger = LogManager.getLogger(getClass)

  logger.info("Jar Manager Actor started successfully!")
  override def receive: Receive = {
    case ListJars => sender ! jobDao.getApps

    case StoreLocalJars(localJars) =>
      val success =
        localJars.foldLeft(true)((succ, pair) =>
          succ && {
            val (appName, jarPath) = pair
            val jarBytes = Files.readAllBytes(Paths.get(jarPath))
            logger.info(s"Storing jar for app $appName, ${jarBytes.length} bytes")
            try {
              JarUtils.validateJarBytes(jarBytes) && {
                saveJar(appName, jarBytes)
                true
              }
            } catch {
              case e: Exception =>
                logger.error("Could not Store local file", e)
                false
            }
          }
        )
      sender ! (if (success) { JarStored } else { InvalidJar })

    case StoreJar(appName, jarBytes) =>
      logger.info(s"Storing jar for app $appName, ${jarBytes.length} bytes")
      if (!JarUtils.validateJarBytes(jarBytes)) {
        sender ! InvalidJar
        logger.warn(s"Store jar $appName failed.")
      } else {
        saveJar(appName, jarBytes)
        sender ! JarStored
        logger.info(s"Store jar $appName successfully.")
      }
  }

  private def saveJar(appName: String, jarBytes: Array[Byte]): Unit = {
    jobDao.saveJar(appName, new Date(), jarBytes)
  }
}