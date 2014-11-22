package com.balihoo.fulfillment.workers

import java.net.{Inet4Address, URI}

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.{JsonSchemaFactory, JsonSchema}
import org.joda.time.DateTime
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
        new EncryptedActivityParameter("param1", "Param 1 is an Ocelot", true),
        new StringActivityParameter("param2", "Param 2 is NOT an Ocelot", false)
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
        new StringActivityParameter("param2", "Param 2 is NOT an Ocelot", false)
      ), new StringActivityResult("really interesting description"))

      spec.getParameters(
        """{
          | "param2" : "all kinds of angry"
          |}
        """.stripMargin) must throwA[Exception](message = "validation error: 'param1' is required")
    }

    "produce a valid jsonSchema for parameters" in {
      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string"),
        new NumberActivityParameter("param2", "Param 2 is a number", false)
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val factory = JsonSchemaFactory.byDefault()

      val schema:JsonSchema = factory.getJsonSchema(spec.parameterSchema.as[JsonNode])

      val input:JsonNode = Json.parse(
        """{
              "param1" : "stuff",
              "param6" : "will be ignored"
          }""").as[JsonNode]

//      println(schema)

      val report = schema.validate(input)

      report.isSuccess

    }

    "be upset about mismatched type" in {

      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string"),
        new NumberActivityParameter("param2", "Param 2 is a number", false)
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val factory = JsonSchemaFactory.byDefault()

      val schema:JsonSchema = factory.getJsonSchema(spec.parameterSchema.as[JsonNode])

      val input =
        """{
              "param1" : "stuff",
              "param2" : "wrong type"
          }"""

      spec.getParameters(input) must throwA[Exception](message = "validation type error: found 'string' expected 'integer, number'")

    }

    "handle a good datetime" in {
      val spec = new ActivitySpecification(List(
        new DateTimeActivityParameter("param1", "ISO8601 goodness")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "2014-11-19T15:48:00-07:00"
          |}
        """.stripMargin

      val params = spec.getParameters(input)

      params[DateTime]("param1") === new DateTime("2014-11-19T15:48:00-07:00")
    }

    "reject a bad datetime" in {
      val spec = new ActivitySpecification(List(
        new DateTimeActivityParameter("param1", "ISO8601 goodness")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "banana peel"
          |}
        """.stripMargin

      spec.getParameters(input) must throwA[Exception].like { case e => e.getMessage must contain("""string \"banana peel\" is invalid against requested date format(s)""") }
    }

    "handle a good URI" in {
      val spec = new ActivitySpecification(List(
        new UriActivityParameter("param1", "A URI")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "http://resumes.balihoo.com/"
          |}
        """.stripMargin

      val params = spec.getParameters(input)

      params[String]("param1") === "http://resumes.balihoo.com/"
    }

    "reject a bad URI" in {
      val spec = new ActivitySpecification(List(
        new UriActivityParameter("param1", "A URI")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "banana peel"
          |}
        """.stripMargin

      spec.getParameters(input) must throwA[Exception].like { case e => e.getMessage must contain("""string \"banana peel\" is not a valid URI""") }
    }

    "handle a good email address" in {
      val spec = new ActivitySpecification(List(
        new EmailActivityParameter("param1", "An email address")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "Tester McTesty <test@balihoo.com>"
          |}
        """.stripMargin

      val params = spec.getParameters(input)

      params[String]("param1") === "Tester McTesty <test@balihoo.com>"
    }

    "reject a bad email address" in {
      val spec = new ActivitySpecification(List(
        new EmailActivityParameter("param1", "An email address")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "banana peel"
          |}
        """.stripMargin

      spec.getParameters(input) must throwA[Exception].like { case e => e.getMessage must contain("""string \"banana peel\" is not a valid email address""") }
    }

    "handle a good IPv4 address" in {
      val spec = new ActivitySpecification(List(
        new Ipv4ActivityParameter("param1", "An IPv4 address")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "1.2.3.4"
          |}
        """.stripMargin

      val params = spec.getParameters(input)

      params[String]("param1") === "1.2.3.4"
    }

    "reject a bad IPv4 address" in {
      val spec = new ActivitySpecification(List(
        new Ipv4ActivityParameter("param1", "An IPv4 address")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "banana peel"
          |}
        """.stripMargin

      spec.getParameters(input) must throwA[Exception].like { case e => e.getMessage must contain("""string \"banana peel\" is not a valid IPv4 address""") }
    }

    "handle a good IPv6 address" in {
      val spec = new ActivitySpecification(List(
        new Ipv6ActivityParameter("param1", "An IPv6 address")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "Dead:Beef:Cafe::bad:f00d:4:a11"
          |}
        """.stripMargin

      val params = spec.getParameters(input)

      params[String]("param1") === "Dead:Beef:Cafe::bad:f00d:4:a11"
    }

    "reject a bad IPv6 address" in {
      val spec = new ActivitySpecification(List(
        new Ipv6ActivityParameter("param1", "An IPv6 address")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "banana peel"
          |}
        """.stripMargin

      spec.getParameters(input) must throwA[Exception].like { case e => e.getMessage must contain("""string \"banana peel\" is not a valid IPv6 address""") }
    }

    "handle a good hostname" in {
      val spec = new ActivitySpecification(List(
        new HostnameActivityParameter("param1", "A hostname")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "test.balihoo.com"
          |}
        """.stripMargin

      val params = spec.getParameters(input)

      params[String]("param1") === "test.balihoo.com"
    }

    "reject a bad hostname" in {
      val spec = new ActivitySpecification(List(
        new HostnameActivityParameter("param1", "A hostname")
      ), new StringActivityResult("Nothing"))

      val input =
        """{
          |  "param1" : "banana peel"
          |}
        """.stripMargin

      spec.getParameters(input) must throwA[Exception].like { case e => e.getMessage must contain("""string \"banana peel\" is not a valid hostname""") }
    }
  }
}
