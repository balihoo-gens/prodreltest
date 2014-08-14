package com.balihoo.fulfillment.workers

import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class TestFulfillmentWorker extends Specification with Mockito
{
  "Test ActivitySpecification" should {
    "be initialized without error" in {

      val spec = new ActivitySpecification(List(
        new ActivityParameter("param1", "ocelot", "Param 1 is an Ocelot"),
        new ActivityParameter("param2", "not an ocelot", "Param 2 is NOT an Ocelot", false)
      ))

      spec.getSpecification mustEqual Json.toJson(Map(
        "param1" -> Json.toJson(Map(
          "name" -> Json.toJson("param1"),
          "type" -> Json.toJson("ocelot"),
          "description" -> Json.toJson("Param 1 is an Ocelot"),
          "required" -> Json.toJson(true)
        )),
        "param2" -> Json.toJson(Map(
          "name" -> Json.toJson("param2"),
          "type" -> Json.toJson("not an ocelot"),
          "description" -> Json.toJson("Param 2 is NOT an Ocelot"),
          "required" -> Json.toJson(false)
        ))
      ))

    }

    "properly filter json input" in {

      val spec = new ActivitySpecification(List(
        new ActivityParameter("param1", "ocelot", "Param 1 is an Ocelot"),
        new ActivityParameter("param2", "not an ocelot", "Param 2 is NOT an Ocelot", false)
      ))

      val params = spec.getParameters(
        """{
          | "param1" : "flesh of the tuna",
          | "not declared" : "stuff we won't get access too"
          |}
        """.stripMargin)

      params.has("param2") mustEqual false
      params.getOrElse("param2", "indigo") mustEqual "indigo"
      params("param1") mustEqual "flesh of the tuna"
      params.getOrElse("param1", "beefeater") mustEqual "flesh of the tuna"
    }

    "be upset about missing params that are required" in {

      val spec = new ActivitySpecification(List(
        new ActivityParameter("param1", "ocelot", "Param 1 is an Ocelot"),
        new ActivityParameter("param2", "not an ocelot", "Param 2 is NOT an Ocelot", false)
      ))

      spec.getParameters(
        """{
          | "param2" : "all kinds of angry"
          |}
        """.stripMargin) must throwA[Exception](message = "input parameter 'param1' is REQUIRED!")
    }
  }
}
