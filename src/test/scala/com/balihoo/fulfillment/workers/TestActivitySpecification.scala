package com.balihoo.fulfillment.workers

import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.github.fge.jsonschema.main.{JsonSchemaFactory, JsonSchema}
import org.keyczar.Crypter
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class TestActivitySpecification extends Specification with Mockito
{
  "Test ActivitySpecification" should {
    "be initialized without error" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is an Ocelot"),
        new NumberActivityParameter("param2", "Param 2 is NOT an Ocelot", false)
      ), new StringActivityResult("really interesting description"),
       "description for the whole activity. Notes and stuff")

      val schema = """{"$schema":"http://json-schema.org/draft-04/schema","type":"object","required":["param1"],"properties":{"param1":{"type":"string","description":"Param 1 is an Ocelot"},"param2":{"type":"number","description":"Param 2 is NOT an Ocelot"}}}"""

      spec.getSpecification mustEqual Json.toJson(Map(
        "parameters" -> Json.toJson(Map(
          "param1" -> Json.toJson(Map(
            "name" -> Json.toJson("param1"),
            "type" -> Json.toJson("string"),
            "description" -> Json.toJson("Param 1 is an Ocelot"),
            "required" -> Json.toJson(true)
          )),
          "param2" -> Json.toJson(Map(
            "name" -> Json.toJson("param2"),
            "type" -> Json.toJson("number"),
            "description" -> Json.toJson("Param 2 is NOT an Ocelot"),
            "required" -> Json.toJson(false)
          )))),
        "result" -> Json.toJson(Map(
          "type" -> Json.toJson("string"),
          "description" -> Json.toJson("really interesting description")
        )),
        "description" -> Json.toJson("description for the whole activity. Notes and stuff"),
        "schema" -> Json.parse(schema)
      )
      )

    }

    "properly filter json input" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is an Ocelot"),
        new NumberActivityParameter("param2", "Param 2 is NOT an Ocelot", false)
      ), new StringActivityResult("really interesting description"))

      val params = spec.getParameters(
        """{
          | "param1" : "flesh of the tuna",
          | "not declared" : "stuff we won't get access too"
          |}
        """.stripMargin)

      params.has("param2") mustEqual false
      params.getOrElse("param2", 675) mustEqual 675
      params[String]("param1") mustEqual "flesh of the tuna"
      params.getOrElse("param1", "beefeater") mustEqual "flesh of the tuna"

    }

    "properly decrypt sensitive parameters" in {

      val spec = new ActivitySpecification(List(
        new EncryptedActivityParameter("param1", "Param 1 is an Ocelot"),
        new StringActivityParameter("param2", "Param 2 is NOT an Ocelot", required=false)
      ), new StringActivityResult("really interesting description"))

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
      params[String]("param1") mustEqual input
      params.getOrElse("param1", "beefeater") mustEqual input
    }

    "be upset about missing params that are required" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is an Ocelot"),
        new StringActivityParameter("param2", "Param 2 is NOT an Ocelot", required=false)
      ), new StringActivityResult("really interesting description"))

      spec.getParameters(
        """{
          | "param2" : "all kinds of angry"
          |}
        """.stripMargin) must throwA(new Exception("""validation error:  object has missing required properties (["param1"])"""))
    }

    "produce a valid jsonSchema for parameters" in {
      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string"),
        new NumberActivityParameter("param2", "Param 2 is a number", required=false)
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val factory = JsonSchemaFactory.byDefault()

      val mapper = new ObjectMapper()
      val schema:JsonSchema = factory.getJsonSchema(mapper.readTree(Json.stringify(spec.parameterSchema)))

      val input:JsonNode = Json.parse(
        """{
              "param1" : "stuff",
              "param6" : "will be ignored"
          }""").as[JsonNode]

      val report = schema.validate(input)

      report.isSuccess

    }

    "be upset about mismatched type" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string"),
        new NumberActivityParameter("param2", "Param 2 is a number", required=false)
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input =
        """{
              "param1" : "stuff",
              "param2" : "wrong type"
          }"""

      spec.getParameters(input) must throwA(new Exception("""validation error: /param2 instance type (string) does not match any allowed primitive type (allowed: ["integer","number"])"""))

    }

    "check enums properly" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string"),
        new EnumActivityParameter("param2", "Param 2 is a number", List("FERRET", "CHICKEN"))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val params = spec.getParameters(
        """{
              "param1" : "stuff",
              "param2" : "FERRET"
          }""")

      params.has("param2") mustEqual true
      params[String]("param2") mustEqual "FERRET"

    }

    "be upset about improper enum" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string"),
        new EnumActivityParameter("param2", "Param 2 is a number", List("FERRET", "CHICKEN"))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{
              "param1" : "stuff",
              "param2" : "HOUDINI"
          }"""

      spec.getParameters(input) must throwA(new Exception("""validation error: /param2 instance value ("HOUDINI") not found in enum (possible values: ["FERRET","CHICKEN"])"""))


    }

    "gripe about multiple errors" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string"),
        new EnumActivityParameter("param2", "Param 2 is a number", List("FERRET", "CHICKEN"))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{
              "param1" : 55,
              "param2" : "HOUDINI"
          }"""

      spec.getParameters(input) must throwA(new Exception("""validation error: /param1 instance type (number) does not match any allowed primitive type (allowed: ["string"])
validation error: /param2 instance value ("HOUDINI") not found in enum (possible values: ["FERRET","CHICKEN"])"""))


    }

    "enforce string maxLength" in {
      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string", maxLength=Some(15))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{
              "param1" : "this string is much longer than 15 characters"
          }"""

      spec.getParameters(input) must throwA(new Exception("""validation error: /param1 string "this string is much longer than 15 characters" is too long (length: 45, maximum allowed: 15)"""))

    }

    "enforce string minLength" in {
      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string", minLength=Some(15))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{
              "param1" : "too short!"
          }"""

      spec.getParameters(input) must throwA(new Exception("""validation error: /param1 string "too short!" is too short (length: 10, required minimum: 15)"""))

    }

    "enforce string pattern" in {
      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string", pattern = Some("[0-9]{3}[a-z]{4}"))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{
              "param1" : "feesh"
          }"""

      spec.getParameters(input) must throwA(new Exception( """validation error: /param1 ECMA 262 regex "[0-9]{3}[a-z]{4}" does not match input string "feesh""""))
    }

      "accept string pattern" in {
        val spec = new ActivitySpecification(List(
          new StringActivityParameter("param1", "Param 1 is a string", pattern=Some("[0-9]{3}[a-z]{4}"))
        ), new StringActivityResult("really interesting description"),
          "description for the whole activity. Notes and stuff")

        val input = """{
              "param1" : "567erty"
          }"""

        val params = spec.getParameters(input)

        params[String]("param1") mustEqual "567erty"
    }
  }
}
