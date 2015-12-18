package etl

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, Matchers, FunSpecLike}
import play.api.mvc.Results

/**
  * Created by king on 15-12-16.
  */
abstract class ETLBaseSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfterAll with Results
