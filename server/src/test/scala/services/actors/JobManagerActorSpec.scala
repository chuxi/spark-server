package services.actors

import java.nio.file.Paths

import akka.actor.Props
import com.typesafe.config.ConfigFactory
import services.InMemoryDAO
import services.actors.JobManagerActor._
import services.protocals.CommonMessages._

import scala.collection.mutable

/**
 * Created by king on 15-10-10.
 */

object JobManagerActorSpec extends JobSpecConfig

class JobManagerActorSpec extends JobSpecBase(JobManagerActorSpec.getNewSystem) {
  import JobManagerActorSpec.MaxJobsPerContext
  import akka.testkit._

  import scala.concurrent.duration._

  val classPrefix = "cn.edu.zju.king.jobserver.test."
  private val wordCountClass = classPrefix + "WordCountExample"
  protected val stringConfig = ConfigFactory.parseString("input.string = The lazy dog jumped over the fish")
  protected val emptyConfig = ConfigFactory.parseString("spark.master = bar")

  before {
    dao = new InMemoryDAO
    manager =
      system.actorOf(Props(classOf[JobManagerActor], dao, "test", JobManagerActorSpec.contextConfig, false, None))
  }

  describe("error conditions") {
    it("should return errors if appName does not match") {
      uploadTestJar()
      manager ! StartJob("demo2", wordCountClass, emptyConfig, Set.empty[Class[_]])
      expectMsg(NoSuchApplication)
    }

    it("should return error message if classPath does not match") {
      uploadTestJar()
      manager ! Initialize
      expectMsgClass(Duration(6, SECONDS), classOf[Initialized])
      manager ! StartJob("demo", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(NoSuchClass)
    }

    it("should error out if loading garbage jar") {
      val cwd = Paths.get(".").toAbsolutePath().normalize().toString()
      uploadJar(dao, "./README.md", "notajar")
      manager ! Initialize
      expectMsgClass(classOf[Initialized])
      manager ! StartJob("notajar", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(NoSuchClass)
    }

    it("should error out if job validation fails") {
      manager ! Initialize
      expectMsgClass(Duration(60, SECONDS), classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", wordCountClass, emptyConfig, allEvents)
      expectMsgClass(Duration(60, SECONDS), classOf[JobValidationFailed])
      expectNoMsg()
    }
  }

  describe("starting jobs") {
    it("should start job and return result successfully (all events)") {
      manager ! Initialize
      expectMsgClass(Duration(600, SECONDS), classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(Duration(600, SECONDS), classOf[JobStarted])
      expectMsgAllClassOf(Duration(600, SECONDS), classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job more than one time and return result successfully (all events)") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()

      // should be ok to run the same more again
      manager ! StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job and return results (sync route)") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Map[_, _]) => println("I got results! " + result)
      }
      expectNoMsg()
    }

    it("should start job and return JobStarted (async)") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", wordCountClass, stringConfig, errorEvents ++ asyncEvents)
      expectMsgClass(classOf[JobStarted])
      expectNoMsg()
    }

    it("should return error if job throws an error") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", classPrefix + "MyErrorJob", emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])
    }

    it("job should get jobConfig passed in to StartJob message") {
      val jobConfig = ConfigFactory.parseString("foo.bar.baz = 3")
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", classPrefix + "ConfigCheckerJob", jobConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, keys: Seq[_]) =>
          keys should contain ("foo")
      }
    }

    it("should properly serialize case classes and other job jar classes") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", classPrefix + "ZookeeperJob", stringConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Array[Product]) =>
          result.length should equal (1)
          result(0).getClass.getName should include ("Animal")
      }
      expectNoMsg()
    }

    it ("should refuse to start a job when too many jobs in the context are running") {
      val jobSleepTimeMillis = 2000L
      val jobConfig = ConfigFactory.parseString("sleep.time.millis = " + jobSleepTimeMillis)

      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()

      val messageCounts = new mutable.HashMap[Class[_], Int].withDefaultValue(0)
      // Try to start 3 instances of this job. 2 of them should start, and the 3rd should be denied.
      for (i <- 0 until MaxJobsPerContext + 1) {
        manager ! StartJob("demo", classPrefix + "SleepJob", jobConfig, allEvents)
      }

      while (messageCounts.values.sum < (MaxJobsPerContext * 3 + 1)) {
        expectMsgPF(3.seconds.dilated, "Expected a message but didn't get one!") {
          case started: JobStarted =>
            messageCounts(started.getClass) += 1
          case noSlots: NoJobSlotsAvailable =>
            noSlots.maxJobSlots should equal (MaxJobsPerContext)
            messageCounts(noSlots.getClass) += 1
          case finished: JobFinished =>
            messageCounts(finished.getClass) += 1
          case result: JobResult =>
            result.result should equal (jobSleepTimeMillis)
            messageCounts(result.getClass) += 1
        }
      }
      messageCounts.toMap should equal (Map(classOf[JobStarted] -> MaxJobsPerContext,
        classOf[JobFinished] -> MaxJobsPerContext,
        classOf[JobResult] -> MaxJobsPerContext,
        classOf[NoJobSlotsAvailable] -> 1))
    }

    it("should start a job that's an object rather than class") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Int) => result should equal (1 + 2 + 3)
      }
    }

    it("should be able to cancel running job") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", classPrefix + "LongPiJob", stringConfig, allEvents)
      expectMsgPF(1.seconds.dilated, "Did not get JobResult") {
        case JobStarted(id, _, _) => {
          manager ! KillJob(id)
          expectMsgClass(classOf[JobKilled])
        }
      }
    }

  }

  describe("starting jobs") {
    it("jobs should be able to cache RDDs and retrieve them through getPersistentRDDs") {
      manager ! Initialize
      expectMsgClass(1000.seconds, classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", classPrefix + "CacheSomethingJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum: Int) = expectMsgClass(1000.seconds, classOf[JobResult])

      manager ! StartJob("demo", classPrefix + "AccessCacheJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum2: Int) = expectMsgClass(classOf[JobResult])

      sum2 should equal (sum)
    }

    it ("jobs should be able to cache and retrieve RDDs by name") {
      manager ! Initialize
      expectMsgClass(classOf[Initialized])

      uploadTestJar()
      manager ! StartJob("demo", classPrefix + "CacheRddByNameJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgPF(1.second.dilated, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, sum: Int) => sum should equal (1 + 4 + 9 + 16 + 25)
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
    }
  }



}
