package controllers

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._
import play.api.mvc.Results

/**
  * Created by king on 15-12-11.
  */
abstract class ControllerBaseSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with Results