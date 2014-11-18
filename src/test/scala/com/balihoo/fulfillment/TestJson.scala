package com.balihoo.fulfillment

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.core.report.{ProcessingMessage, ProcessingReport}
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions
import collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestJson extends Specification with Mockito
{
  "JsonSchema" should {
    "interopt with JsValue" in {

      val addressSchema:Option[JsonNode] = Json.parse(
        """{
              "$schema": "http://json-schema.org/draft-03/schema",
              "type": ["object","string"],
              "properties": {
                "address": {
                  "type": "object",
                  "properties": {
                    "city": {
                      "type": "string"
                    },
                    "streetAddress": {
                      "type": "string"
                    }
                  }
                },
                "age":{
                  "required" : true,
                  "type": "number"
                },
                "phoneNumber": {
                  "type" : "array",
                  "items" : {
                    "required":false,
                    "type" : "object",
                    "properties" : {
                      "number" : {
                        "required": true,
                        "type" : "string"
                      },
                      "type": {
                        "required" : false,
                        "type" : "string"
                      }
                    }
                  }
                }
              }
            }""").asOpt[JsonNode]

      val good:JsonNode = Json.parse(
        """{
              "address":{
                "streetAddress": "21 2nd Street",
                "city":"New York"
              },
              "phoneNumber":
              [
                {
                  "type":"home",
                  "number":"212 555-1234"
                }
              ],
              "age" : 77
          }""").as[JsonNode]

      val good2:JsonNode = Json.parse(
        """"just a simple string"""").as[JsonNode]

      val bad:JsonNode = Json.parse(
        """{
              "address":{
                "streetAddress": "21 2nd Street",
                "city":6675876
              },
              "phoneNumber":
              [
                {
                  "type":"home",
                  "number":"212 555-1234"
                }
              ]
          }""").as[JsonNode]

      val factory = JsonSchemaFactory.byDefault()

      val schema:JsonSchema = factory.getJsonSchema(addressSchema.get)

      val goodReport:ProcessingReport = schema.validate(good)
      goodReport.isSuccess mustEqual true

      val goodReport2:ProcessingReport = schema.validate(good2)
      goodReport2.isSuccess mustEqual true

      val badReport:ProcessingReport = schema.validate(bad)

      badReport.isSuccess mustEqual false

      for(m:ProcessingMessage <- badReport) {
        val report = Json.toJson(m.asJson).as[JsObject]
        report.value("domain").as[String] mustEqual "validation"
        report.value("required").as[List[String]] mustEqual List("age")
//        for((k, v) <- Json.toJson(m.asJson).as[JsObject].fields) {
//          println(s"$k:$v")
//        }
      }

      true

    }
  }
}
