package services.util

import java.lang.reflect.Constructor

import org.apache.logging.log4j.LogManager

/**
 * Created by king on 15-9-19.
 */
object JarUtils {
  val logger = LogManager.getLogger(getClass)

  def loadClassOrObject[C](classOrObjectName: String, loader: ClassLoader): () => C = {
    def fallBackToClass(): () => C = {
      val constructor = loadConstructor[C](classOrObjectName, loader)
      () => constructor.newInstance()
    }

    try {
      val objectRef = loadObject[C](classOrObjectName + "$", loader)
      () => objectRef
    } catch {
      case e: ClassNotFoundException => fallBackToClass()
      case e: ClassCastException => fallBackToClass()
      case e: NoSuchMethodException => fallBackToClass()
      case e: NoSuchFieldException => fallBackToClass()
    }
  }

  private def loadConstructor[C](className: String, loader: ClassLoader): Constructor[C] = {
    logger.info("Loading class {} using loader {}", className, loader)
    val loadedClass = loader.loadClass(className).asInstanceOf[Class[C]]
    val result = loadedClass.getConstructor()
    if (loadedClass.getClassLoader != loader) {
      logger.error("Wrong ClassLoader for class {}: Expected {} but got {}",
        loadedClass.getName, loader.toString, loadedClass.getClassLoader)
    }
    result
  }

  private def loadObject[C](objectName: String, loader: ClassLoader): C = {
    logger.info("Loading Object {} using loader {}", objectName, loader)
    val loadedClass = loader.loadClass(objectName)
    val objectRef = loadedClass.getField("MODULE$").get(null).asInstanceOf[C]
    if (objectRef.getClass.getClassLoader != loader) {
      logger.error("Wrong ClassLoader for class {}: Expected {} but got {}",
        objectRef.getClass.getName, loader.toString, objectRef.getClass.getClassLoader)
    }
    objectRef
  }



  def validateJarBytes(jarBytes: Array[Byte]): Boolean = {
    jarBytes.length > 4 &&
      // For now just check the first few bytes are the ZIP signature: 0x04034b50 little endian
      jarBytes(0) == 0x50 && jarBytes(1) == 0x4b && jarBytes(2) == 0x03 && jarBytes(3) == 0x04
  }

}
