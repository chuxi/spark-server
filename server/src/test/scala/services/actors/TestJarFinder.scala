package services.actors

import java.nio.file.Paths

/**
 * Created by king on 15-10-10.
 */
trait TestJarFinder {
  val versionRegex = """(\d\.\d+).*""".r
  val version = scala.util.Properties.versionNumberString match { case versionRegex(d) => d }
  val testJarBaseDir = "algolibs"
  lazy val testJarDir = testJarBaseDir + "/target/scala-" + version + "/"

  def baseDir: String = {
    // Current directory.  Find out if we are in project root, and need to go up a level.
    val cwd = Paths.get(".").toAbsolutePath().normalize().toString()
    val dotdot = if (Paths.get(cwd + "/algolibs").toFile.isDirectory) "" else "../"
    s"$cwd/$dotdot"
  }

  // Make testJar lazy so to give a chance for overriding of testJarDir to succeed
  lazy val testJar: java.io.File = {
    val candidates = new java.io.File(baseDir + testJarDir).listFiles.toSeq
    val allJars = candidates.filter { file =>
      val path = file.toString
      path.endsWith(".jar") && !path.endsWith("-tests.jar") && !path.endsWith("-sources.jar") &&
        !path.endsWith("-javadoc.jar") && !path.contains("scoverage")
    }
    assert(allJars.size == 1, allJars.toList.toString())
    allJars.head
  }
}
