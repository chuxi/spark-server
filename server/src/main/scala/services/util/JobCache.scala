package services.util

import java.net.URL
import java.util.Date

import cn.edu.zju.king.serverapi.SparkJobBase
import org.apache.spark.SparkContext
import services.io.JobDAO

/**
 * Created by king on 15-9-22.
 */

case class JobJarInfo(constructor: () => SparkJobBase, className: String, jarFilePath: String)

class JobCache(maxEntries: Int, dao: JobDAO, sparkContext: SparkContext, loader: ContextURLClassLoader) {
  private val cache = new LRUCache[(String, Date, String), JobJarInfo](maxEntries)

  def getSparkJob(appName: String, uploadTime: Date, classPath: String): JobJarInfo = {
    cache.get((appName, uploadTime, classPath), {
      val jarFilePath = dao.retrieveJarFile(appName, uploadTime)
      sparkContext.addJar(jarFilePath)
      loader.addURL(new URL("file:" + jarFilePath))
      val constructor = JarUtils.loadClassOrObject[SparkJobBase](classPath, loader)
      JobJarInfo(constructor, classPath, jarFilePath)
    })
  }

}
