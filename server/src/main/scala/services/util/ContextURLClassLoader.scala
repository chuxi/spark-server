package services.util

import java.net.{URL, URLClassLoader}

import org.apache.logging.log4j.LogManager

/**
 * Created by king on 15-9-22.
 */
class ContextURLClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  private val logger = LogManager.getLogger(getClass)

  override def addURL(url: URL): Unit = {
    if (!getURLs.contains(url)) {
      super.addURL(url)
      logger.info("Added URL " + url + " to ContextURLClassLoader")
    }
  }

}
