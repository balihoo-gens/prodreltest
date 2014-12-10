package com.balihoo.fulfillment.workers

import java.net.URI

import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
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
        """.stripMargin) must throwA(ActivitySpecificationException("""validation error:  object has missing required properties (["param1"])"""))
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

      spec.getParameters(input) must throwA(ActivitySpecificationException("""validation error: /param2 instance type (string) does not match any allowed primitive type (allowed: ["integer","number"])"""))

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

      spec.getParameters(input) must throwA(ActivitySpecificationException("""validation error: /param2 instance value ("HOUDINI") not found in enum (possible values: ["FERRET","CHICKEN"])"""))


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

      spec.getParameters(input) must throwA(ActivitySpecificationException("""validation error: /param1 instance type (integer) does not match any allowed primitive type (allowed: ["string"])
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

      spec.getParameters(input) must throwA(ActivitySpecificationException("""validation error: /param1 string "this string is much longer than 15 characters" is too long (length: 45, maximum allowed: 15)"""))

    }

    "enforce string minLength" in {
      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string", minLength=Some(15))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{
              "param1" : "too short!"
          }"""

      spec.getParameters(input) must throwA(ActivitySpecificationException("""validation error: /param1 string "too short!" is too short (length: 10, required minimum: 15)"""))

    }

    "enforce string pattern" in {
      val spec = new ActivitySpecification(List(
        new StringActivityParameter("param1", "Param 1 is a string", pattern = Some("[0-9]{3}[a-z]{4}"))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{
              "param1" : "feesh"
          }"""

      spec.getParameters(input) must throwA(ActivitySpecificationException( """validation error: /param1 ECMA 262 regex "[0-9]{3}[a-z]{4}" does not match input string "feesh""""))
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

      spec.getParameters(input) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""string "banana peel" is invalid against requested date format(s)""") }
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

      params[URI]("param1") === new URI("http://resumes.balihoo.com/")
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

      spec.getParameters(input) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""string "banana peel" is not a valid URI""") }
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

      spec.getParameters(input) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""string "banana peel" is not a valid email address""") }
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

      spec.getParameters(input) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""string "banana peel" is not a valid IPv4 address""") }
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

      spec.getParameters(input) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""string "banana peel" is not a valid IPv6 address""") }
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

      spec.getParameters(input) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""string "banana peel" is not a valid hostname""") }
    }

    "parse integer type" in {
      val spec = new ActivitySpecification(List(
        new IntegerActivityParameter("param1", "Param 1 is an integer")
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{"param1" : 1}"""
      val badInput = """{"param1" : [5]}"""

      val params = spec.getParameters(input)
      params[Int]("param1") must beEqualTo(1)

      spec.getParameters(badInput) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""/param1 instance type (array) does not match any allowed primitive type (allowed: ["integer"])""") }
    }

    "parse long type" in {
      val spec = new ActivitySpecification(List(
        new LongActivityParameter("param1", "Param 1 is a long")
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{"param1" : 1}"""
      val badInput = """{"param1" : "stork hip"}"""

      val params = spec.getParameters(input)
      params[Long]("param1") must beEqualTo(1.toLong)

      spec.getParameters(badInput) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""param1 instance type (string) does not match any allowed primitive type (allowed: ["integer"])""") }
    }

    "parse enums type" in {

      val spec = new ActivitySpecification(List(
        new EnumsActivityParameter("param1", "Param 1 is an enumses", options=List("BEETLE", "JUICE", "IS", "WEIRD"))
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input = """{"param1" : ["IS", "BEETLE"]}"""
      val badinput = """{"param1" : ["IS", "BEATLE"]}"""

      val params = spec.getParameters(input)
      params[List[String]]("param1") mustEqual List("IS", "BEETLE")

      spec.getParameters(badinput) must throwA[ActivitySpecificationException].like { case e => e.getMessage must contain("""instance value ("BEATLE") not found in enum (possible values: ["BEETLE","JUICE","IS","WEIRD"])""")}
    }

    "validate complex objects" in {

      val spec = new ActivitySpecification(List(
        new ObjectActivityParameter("address", "An address.", properties=List(
          new StringActivityParameter("street", "Street and Number"),
          new StringActivityParameter("city", "The local municipality"),
          new StringActivityParameter("postalCode", "the zip or whatever, dang", pattern=Some("[0-9]{5}"))
        )),

        new StringActivityParameter("favoriteFruit", "Tell me your favorite fruit")
        ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input =
        """{"favoriteFruit" : "Milk bone",
          | "address" : {
          |   "street" : "car named desire",
          |   "city" : "that never sleeps",
          |   "postalCode" : "66677"
          | }
          |}""".stripMargin

      val badinput =
        """{"favoriteFruit" : "Milk bone",
          | "address" : {
          |   "street" : "car named desire",
          |   "city" : "that never sleeps",
          |   "postalCode" : "m6677illegal"
          | }
          |}""".stripMargin


      val params = spec.getParameters(input)
      val addr = params[ActivityParameters]("address")
      addr[String]("city") mustEqual "that never sleeps"

      spec.getParameters(badinput) must throwA[Exception].like { case e => e.getMessage must contain("""/address/postalCode ECMA 262 regex "[0-9]{5}"""")}

    }

    "validate array" in {

      val spec = new ActivitySpecification(List(
        new ArrayActivityParameter("zips", "An bunch of zips",
          element=new StringActivityParameter("postalCode", "the zip or whatever, dang", pattern=Some("[0-9]{5}"))),
        new StringActivityParameter("favoriteFruit", "Tell me your favorite fruit")
      ), new StringActivityResult("really interesting description"),
        "description for the whole activity. Notes and stuff")

      val input =
        """{"favoriteFruit" : "Milk bone",
          | "zips" : [ "90210", "83702", "55566" ]
          |}""".stripMargin

      val badinput =
        """{"favoriteFruit" : "Milk bone",
          | "zips" : [ "90210", "83702", "555dfgh66" ]
          |}""".stripMargin


      val params = spec.getParameters(input)
      params[List[Any]]("zips") contains "90210" mustEqual true

      spec.getParameters(badinput) must throwA[Exception].like { case e => e.getMessage must contain("""/zips/2 ECMA 262 regex "[0-9]{5}"""")}

    }
  }
}
