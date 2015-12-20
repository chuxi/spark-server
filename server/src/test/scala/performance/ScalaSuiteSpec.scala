package performance

import org.scalatest.{FunSpec, BeforeAndAfterAll, Suite}

/**
  * Created by king on 15-12-19.
  */
class ScalaSuiteSpec extends FunSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    println("before all")
  }

  override def afterAll(): Unit = {
    println("after all")
  }

  ignore("hello") {
    ignore("layer 1") {
      it("in hello") {
        println("i am in layer 1 hello")
      }

      it("in hello 2") {
        println("i am in layer 1 hello 2")
      }
    }

    describe("layer 2") {
      it("in hello") {
        println("i am in layer 2 hello")
      }

      it("in hello 2") {
        println("i am in layer 2 hello 2")
      }
    }

  }

  describe("world") {
    it("in world") {
      println("i am in world")
    }
  }



}
