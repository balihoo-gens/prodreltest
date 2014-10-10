package com.balihoo.fulfillment.workers

import org.keyczar.Crypter
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
        new ActivityParameter("param2", "not an ocelot", "Param 2 is NOT an Ocelot", false, true)
      ), new ActivityResult("result type", "really interesting description"),
       "description for the whole activity. Notes and stuff")

      spec.getSpecification mustEqual Json.toJson(Map(
        "parameters" -> Json.toJson(Map(
          "param1" -> Json.toJson(Map(
            "name" -> Json.toJson("param1"),
            "type" -> Json.toJson("ocelot"),
            "description" -> Json.toJson("Param 1 is an Ocelot"),
            "required" -> Json.toJson(true),
            "sensitive" -> Json.toJson(false)
          )),
          "param2" -> Json.toJson(Map(
            "name" -> Json.toJson("param2"),
            "type" -> Json.toJson("not an ocelot"),
            "description" -> Json.toJson("Param 2 is NOT an Ocelot"),
            "required" -> Json.toJson(false),
            "sensitive" -> Json.toJson(true)
          )))),
        "result" -> Json.toJson(Map(
          "type" -> Json.toJson("result type"),
          "description" -> Json.toJson("really interesting description"),
          "sensitive" -> Json.toJson(false)
        )),
        "description" -> Json.toJson("description for the whole activity. Notes and stuff")
      ))

    }

    "properly filter json input" in {

      val spec = new ActivitySpecification(List(
        new ActivityParameter("param1", "ocelot", "Param 1 is an Ocelot"),
        new ActivityParameter("param2", "not an ocelot", "Param 2 is NOT an Ocelot", false)
      ), new ActivityResult("result type", "really interesting description"))

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

    "properly decrypt sensitive parameters" in {

      val spec = new ActivitySpecification(List(
        new ActivityParameter("param1", "ocelot", "Param 1 is an Ocelot", true, true),
        new ActivityParameter("param2", "not an ocelot", "Param 2 is NOT an Ocelot", false)
      ), new ActivityResult("result type", "really interesting description"))

      val input = "super secret secrets about tuna flesh"
      val crypter = new Crypter("config/crypto")
      val ciphertext = crypter.encrypt(input)

      val params = spec.getParameters(
        s"""{
          | "param1" : "$ciphertext",
          | "not declared" : "stuff we won't get access too"
          |}
        """.stripMargin)

      params.has("param2") mustEqual false
      params.getOrElse("param2", "indigo") mustEqual "indigo"
      params("param1") mustEqual input
      params.getOrElse("param1", "beefeater") mustEqual input
    }

    "be upset about missing params that are required" in {

      val spec = new ActivitySpecification(List(
        new ActivityParameter("param1", "ocelot", "Param 1 is an Ocelot"),
        new ActivityParameter("param2", "not an ocelot", "Param 2 is NOT an Ocelot", false)
      ), new ActivityResult("result type", "really interesting description"))

      spec.getParameters(
        """{
          | "param2" : "all kinds of angry"
          |}
        """.stripMargin) must throwA[Exception](message = "input parameter 'param1' is REQUIRED!")
    }
  }
}
