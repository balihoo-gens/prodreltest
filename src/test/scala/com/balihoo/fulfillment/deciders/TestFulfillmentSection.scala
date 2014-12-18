package com.balihoo.fulfillment.deciders

import org.joda.time.DateTime

import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class TestFulfillmentSection extends Specification with Mockito
{

  "FulfillmentSection" should {
    "  be initialized without error" in {

      val json = Json.parse( """{
         "cake" : {
         "action" : { "name" : "bake",
                      "version" : "1",
                      "failure" : {
                          "max" : "3",
                          "delay" : "100"
                      },
                      "cancelation" : {
                          "max" : "7",
                          "delay" : "4535"
                      },
                      "timeout" : {
                          "max" : "14",
                          "delay" : "0"
                      },
                      "startToCloseTimeout" : "tuna sandwich"
                    },
         "params" : { "cake_batter" : { "<(section)>" : "batter"  },
				              "cake_pan" : "9\" x 11\"",
				              "bake_time" : "40" },
         "prereqs" : ["heat_oven"],
         "status" : "READY",
         "waitUntil" : "2093-07-04T16:04:00-06",
         "totally unhandled" : "stuff"
	      }}""")


      val jso = json.as[JsObject].value("cake").as[JsObject]
      val section = new FulfillmentSection("test section", jso, DateTime.now)

      section.name mustEqual "test section"
      section.prereqs(0) mustEqual "heat_oven"
      section.action.get.getName mustEqual "bake"
      section.action.get.getVersion mustEqual "1"
      section.failureParams.maxRetries mustEqual 3
      section.failureParams.delaySeconds mustEqual 100
      section.cancelationParams.maxRetries mustEqual 7
      section.cancelationParams.delaySeconds mustEqual 4535
      section.timeoutParams.maxRetries mustEqual 14
      section.timeoutParams.delaySeconds mustEqual 0

      section.scheduledCount mustEqual 0
      section.failedCount mustEqual 0
      section.timedoutCount mustEqual 0
      section.canceledCount mustEqual 0
      section.startedCount mustEqual 0

      section.startToCloseTimeout mustEqual Some("tuna sandwich")
      section.waitUntil.get mustEqual new DateTime("2093-07-04T16:04:00-06")

//      section.params("cake_batter").asInstanceOf[SectionReferences].sections(0).name mustEqual "batter"
//      section.params("cake_pan").asInstanceOf[String] mustEqual "9\" x 11\""
//      section.params("bake_time").asInstanceOf[String] mustEqual "40"

      section.status mustEqual SectionStatus.withName("READY")

      section.timeline.events(0).message mustEqual "totally unhandled : \"stuff\""
    }

    "  handle status changes" in {

      val json = Json.parse( """{
         "action" : { "name" : "awesome",
                      "version" : "a zillion",
                      "failure" : {
                          "max" : "1",
                          "delay" : "100"
                      },
                      "cancelation" : {
                          "max" : "2",
                          "delay" : "4535"
                      },
                      "timeout" : {
                          "max" : "3",
                          "delay" : "0"
                      },
                      "startToCloseTimeout" : "tuna sandwich"
                    },
         "params" : { "param1" : "1",
				              "param2" : "2"},
         "prereqs" : ["heat_oven"],
         "status" : "READY"
	      }""").as[JsObject]

      val section = new FulfillmentSection("sectionName", json, DateTime.now)

      section.status mustEqual SectionStatus.READY

      section.startedCount mustEqual 0
      section.setStarted(DateTime.now)

      section.status mustEqual SectionStatus.STARTED

      section.startedCount mustEqual 1

      section.setStarted(DateTime.now)
      section.setStarted(DateTime.now)

      section.startedCount mustEqual 3
      section.status mustEqual SectionStatus.STARTED

      section.setScheduled(DateTime.now)
      section.status mustEqual SectionStatus.SCHEDULED

      section.scheduledCount mustEqual 1
      section.startedCount mustEqual 3 // STILL 3

      section.setCompleted("awesome results", DateTime.now)

      section.value mustEqual JsString("awesome results")
      section.scheduledCount mustEqual 1 // STILL 1
      section.startedCount mustEqual 3 // STILL 3
      section.status mustEqual SectionStatus.COMPLETE

      section.setFailed("reasons", "terrible reasons", DateTime.now)
      section.status mustEqual SectionStatus.FAILED
      section.timeline.events.last.message mustEqual "Failed because:reasons terrible reasons"

      section.setFailed("reasons", "more terrible reasons", DateTime.now)
      section.status mustEqual SectionStatus.TERMINAL
      section.timeline.events.last.message mustEqual "Failed too many times! (2 > 1)"
    }
  }

  "SectionReference" should {
    "  return simple values" in {
      val result = Json.parse("""{
            "value" : "donkey teeth"
	        }""")

      val reference = new SectionReference("none")

      ReferencePath.isJsonPath("stuff") mustEqual false
      ReferencePath.isJsonPath("stuff/tea") mustEqual true
      ReferencePath.isJsonPath("stuff[0]/") mustEqual true
      ReferencePath.isJsonPath("stuff/monkey[9]") mustEqual true
      ReferencePath.isJsonPath("stuff/monkey[9]/cheese") mustEqual true

      reference.section = Some(new FulfillmentSection("some section", result.as[JsObject], DateTime.now))

      reference.getValue mustEqual JsString("donkey teeth")

    }

    "  parse paths properly" in {
      val reference = new SectionReference("none/first/second[3]/fourth")

      reference.name mustEqual "none"
      reference.path.get.getComponent(0).key.get mustEqual "first"
      reference.path.get.getComponent(1).key.get mustEqual "second"
      reference.path.get.getComponent(2).index.get mustEqual 3
      reference.path.get.getComponent(3).key.get mustEqual "fourth"

    }

    "  return json value from jsobject" in {

      val result = """{
            "first" : { "second" : ["u", "o'brien", "y", { "fourth" : "donkey tooth" } ] }
	        }"""
      val sectionJson = Json.parse(s"""{}""")

      val reference = new SectionReference("none/first/second[3]/fourth")
      reference.section = Some(new FulfillmentSection("some section", sectionJson.as[JsObject], DateTime.now))
      reference.section.get.setCompleted(result, DateTime.now())

      reference.getValue mustEqual JsString("donkey tooth")

      val reference2 = new SectionReference("none/first/second/[1]")
      reference2.section = Some(new FulfillmentSection("some section", sectionJson.as[JsObject], DateTime.now))
      reference2.section.get.setCompleted(result, DateTime.now())

      reference2.getValue mustEqual JsString("o'brien")
    }
  }

  "SectionParameter" should {
    "  pass through json" in {
      val input =
        """{"one":"anti-one","two":"anti-two"}"""

      val param = new SectionParameter(Json.obj(
        "one" -> "anti-one",
        "two" -> "anti-two"
      ))

      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual input

    }

    "  evaluate simple operator without map" in {

      val input =
        """{
          |"one" : "anti-one",
          |"two" : { "<(MD5)>" : "bleh"}
          |}""".stripMargin

      val expected =
        """{"one":"anti-one","two":"4EB20288AFAED97E82BDE371260DB8D8"}"""

      val param = new SectionParameter(Json.parse(input))

      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected
    }

    "  be upset about bad parameter" in {

      val input =
        """{
          |"one" : "anti-one",
          |"two" : { "<(MD5)>" : { "what do I do with an object": "tuna" } }
          |}""".stripMargin

      val param = new SectionParameter(Json.parse(input))

      param.evaluate(null, Map[String, JsValue]())

      param.isResolvable mustEqual false

    }

    "  evaluate compound operator" in {

      val input =
        """{
          |"one" : "anti-one",
          |"two" : { "<(MD5)>" :{ "<(StringFormat)>" : { "format" : "b{replacehere}h", "replacehere" : "le" } }
          |}
          |}""".stripMargin

      val expected =
        """{"one":"anti-one","two":"4EB20288AFAED97E82BDE371260DB8D8"}"""

      val param = new SectionParameter(Json.parse(input))

      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected
    }

    "  get object keys" in {

      val input =
        """{"<(ObjectKeys)>" : { "stork" : "ankles", "cellar" : "door" }}"""

      val expected =
        """["stork","cellar"]"""

      val param = new SectionParameter(Json.parse(input))

      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected
    }

    "  get object values" in {

      val input =
        """{"<(ObjectValues)>" : { "stork" : "ankles", "cellar" : "door" }}"""

      val expected =
        """["ankles","door"]"""

      val param = new SectionParameter(Json.parse(input))
      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected
    }

    "  format strings with StringFormat" in {
      val input = """{
            "<(StringFormat)>" :
              { "format" : "The {subject} eats {food} when {time}",
                         "subject" : "GIANT",
                         "food" : "tuna flesh",
                         "time" : "whenever the hail he wants..!!",
                         "pointless" : "this won't get used" }
	        }"""

       val expected =
        """"The GIANT eats tuna flesh when whenever the hail he wants..!!""""

      val param = new SectionParameter(Json.parse(input))
      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected

    }

    "  be upset about " in {
      val input = """{
            "<(StringFormat)>" :
              { "format" : "The {subject} eats {food} when {time}",
                         "subject" : "GIANT",
                         "food" : "tuna flesh",
                         "time" : "whenever the hail he wants..!!",
                         "pointless" : "this won't get used" }
	        }"""

      val expected =
        """"The GIANT eats tuna flesh when whenever the hail he wants..!!""""

      val param = new SectionParameter(Json.parse(input))
      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected

    }

    "  URL Encode strings using URLEncode" in {
      val str = "if ((!flip && flap->flop()) || (*deref::ptr)[7]) { return ~(&address); }"
      val input =
        s"""{
            "<(URLEncode)>" :
                "$str"
        }"""

      val expected =
        """"if+%28%28%21flip+%26%26+flap-%3Eflop%28%29%29+%7C%7C+%28*deref%3A%3Aptr%29%5B7%5D%29+%7B+return+%7E%28%26address%29%3B+%7D""""

      val param = new SectionParameter(Json.parse(input))
      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected
    }

    "  fetch elements from json with jsonpath" in {
      val str = "gravy/on[3]/cellar/door"
      val input =
        s"""{
            "<(jsonPATH)>" : {
              "path" : "$str",
               "json" : {
                  "gravy" : {
                    "on" : [ 0, 1, 2,
                      { "cellar" : { "door" : "HAYOO" } }
                    ]
                  }
               }
            }
        }"""

      val expected =
        """"HAYOO""""

      val param = new SectionParameter(Json.parse(input))
      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.getResult.get) mustEqual expected
    }

    "  fetch context information" in {
      val input =
        s"""{
            "<(context)>" : "sloth foot"
        }"""

      val expected =
        """"with gravy""""

      val param = new SectionParameter(Json.parse(input))
      param.evaluate(null, Map("sloth foot" -> JsString("with gravy")))
      Json.stringify(param.getResult.get) mustEqual expected
    }

    "  be upset about fetch context information" in {
      val input =
        s"""{
            "<(context)>" : "sloth feets"
        }"""

      val expected =
        """"with gravy""""

      val param = new SectionParameter(Json.parse(input))
      param.evaluate(null, Map("sloth foot" -> JsString("with gravy")))

      !param.isResolved

    }

    "  handle specification of multi param list" in {

      val json = Json.parse( """{
         "action" : { "name" : "awesome",
                      "version" : "a zillion",
                      "failure" : {
                          "max" : "1",
                          "delay" : "100"
                      },
                      "cancelation" : {
                          "max" : "2",
                          "delay" : "4535"
                      },
                      "timeout" : {
                          "max" : "3",
                          "delay" : "0"
                      },
                      "startToCloseTimeout" : "tuna sandwich"
                    },
         "params" : {
                      "multiparam" : [ "carrot", "banana", "faucet" ],
                      "param1" : {
            "<(StringFormat)>" :
                      { "format" : "The {subject} eats {food} when {time}",
                         "subject" : "GIANT",
                         "food" : { "<(context)>" : "multi-value" },
                         "time" : "whenever the hail he wants..!!",
                         "pointless" : "this won't get used" } },

				              "param2" : "2"

                  },
         "multiParam" : "multiparam",
         "prereqs" : [],
         "status" : "READY"
	      }""").as[JsObject]

      val section = new FulfillmentSection("sectionname", json, DateTime.now())

      val fulfillment = new Fulfillment(List())
      fulfillment.nameToSection("sectionname") = section
      section.evaluateParameters(fulfillment)

      fulfillment.categorize()

      fulfillment.getSectionByName("sectionname[0]").parent.get mustEqual fulfillment.getSectionByName("sectionname")
      fulfillment.getSectionByName("sectionname[0]").params("multiparam").getResult.get mustEqual JsString("carrot")

      fulfillment.getSectionByName("sectionname[1]").parent.get mustEqual fulfillment.getSectionByName("sectionname")
      fulfillment.getSectionByName("sectionname[1]").params("multiparam").getResult.get mustEqual JsString("banana")

      fulfillment.getSectionByName("sectionname[2]").parent.get mustEqual fulfillment.getSectionByName("sectionname")
      fulfillment.getSectionByName("sectionname[2]").params("multiparam").getResult.get mustEqual JsString("faucet")

    }

    "  handle specification of multi param object" in {

      val json = Json.parse( """{
         "action" : { "name" : "awesome",
                      "version" : "a zillion",
                      "failure" : {
                          "max" : "1",
                          "delay" : "100"
                      },
                      "cancelation" : {
                          "max" : "2",
                          "delay" : "4535"
                      },
                      "timeout" : {
                          "max" : "3",
                          "delay" : "0"
                      },
                      "startToCloseTimeout" : "tuna sandwich"
                    },
         "params" : {
                      "multiparam" : { "carrot" : "cww___", "banana" : "bn___" },
                      "param1" : {
            "<(StringFormat)>" :
                      { "format" : "The {subject} eats {food} when {time}",
                         "subject" : "GIANT",
                         "food" : { "<(context)>" : "multi-key" },
                         "time" : "whenever the hail he wants..!!",
                         "pointless" : "this won't get used" } },

				              "param2" : "2"

                  },
         "multiParam" : "multiparam",
         "prereqs" : [],
         "status" : "READY"
	      }""").as[JsObject]

      val section = new FulfillmentSection("sectionname", json, DateTime.now())

      val fulfillment = new Fulfillment(List())
      fulfillment.nameToSection("sectionname") = section
      section.evaluateParameters(fulfillment)

      fulfillment.categorize()

      fulfillment.getSectionByName("sectionname[carrot]").parent.get mustEqual fulfillment.getSectionByName("sectionname")
      fulfillment.getSectionByName("sectionname[carrot]").params("multiparam").getResult.get mustEqual JsString("cww___")

      fulfillment.getSectionByName("sectionname[banana]").parent.get mustEqual fulfillment.getSectionByName("sectionname")
      fulfillment.getSectionByName("sectionname[banana]").params("multiparam").getResult.get mustEqual JsString("bn___")

    }

    "  Parse a json string " in {
      val str = """{ \"key\" : [true, null, 25.8, \"string\", { \"object\" : 42 } ] }"""
      val json = Json.parse(s"""{
            "<(jsonparse)>" : "$str"
        }""").as[JsObject]

      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val jsonresult = param.getResult.get
      ((jsonresult \ "key")(4) \ "object").as[Double] must beEqualTo(42)
    }

    "  Stringify a JSON object " in {
      val str = """{"key":[true,null,25.8,"string",{"object":42}]}"""
      val json = Json.parse(s"""{
            "<(jsonstringify)>" : $str
        }""").as[JsObject]

      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val strresult = param.getResult.get.as[String]
      strresult must beEqualTo(str)
    }

    "  get the substring " in {
      val json = Json.parse(s"""{
            "<(substring)>" : {
              "input" : "een aap die geen bananen eet",
              "beginIndex" : 4,
              "endIndex" : 7
            }
        }""").as[JsObject]
      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val strresult = param.getResult.get.as[String]
      strresult must beEqualTo("aap")
    }

    "  replace values " in {
      val json = Json.parse(s"""{
            "<(replace)>" : {
              "input" : "een aap die geen bananen eet",
              "regex" : "aap",
              "replacement" : "kanarie"
            }
        }""").as[JsObject]
      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val strresult = param.getResult.get.as[String]
      strresult must beEqualTo("een kanarie die geen bananen eet")
    }

    "  pick the right value " in {
      val json = Json.parse(s"""{
            "<(switch)>" : {
              "expression" : "Hannibal",
              "BA"      : "I pity the fool that picks me!",
              "Face"    : "Got better things to do",
              "Murdock" : { "I" : ["am", "crazy"] },
              "Hannibal": "I love it when a plan comes together"
            }
        }""").as[JsObject]
      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val strresult = param.getResult.get.as[String]
      strresult must beEqualTo("I love it when a plan comes together")
    }

    "  convert various values to strings" in {

      val json = Json.parse(s"""{
            "<(toString)>" : 555
        }""").as[JsObject]
      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val strresult = param.getResult.getOrElse(JsString("nope!")).as[String]
      strresult must beEqualTo("555")

      val json1 = Json.parse(s"""{
            "<(toString)>" : 555.6666
        }""").as[JsObject]
      val param1 = new SectionParameter(json1)
      param1.evaluate(null, Map[String, JsValue]())
      val strresult1 = param1.getResult.getOrElse(JsString("nope!")).as[String]
      strresult1 must beEqualTo("555.6666")

      val json2 = Json.parse(s"""{
            "<(toString)>" : "555.7777"
        }""").as[JsObject]
      val param2 = new SectionParameter(json2)
      param2.evaluate(null, Map[String, JsValue]())
      val strresult2 = param2.getResult.getOrElse(JsString("nope!")).as[String]
      strresult2 must beEqualTo("555.7777")

      val json3 = Json.parse(s"""{
            "<(toString)>" : [ "a", 6, ["b", 4, false]]
        }""").as[JsObject]
      val param3 = new SectionParameter(json3)
      param3.evaluate(null, Map[String, JsValue]())
      val strresult3 = param3.getResult.getOrElse(JsString("nope!")).as[String]
      // "a" is expected.. cause it processes args(0).. which is the first one in the case
      // of an incoming array
      strresult3 must beEqualTo("a")

      val json4 = Json.parse(s"""{
            "<(toString)>" : { "a": 6, "mooneee" : ["b", 4, false] }
        }""").as[JsObject]
      val param4 = new SectionParameter(json4)
      param4.evaluate(null, Map[String, JsValue]())
      val strresult4 = param4.getResult.getOrElse(JsString("nope!")).as[String]
      // "nope!" is expected.. cause it processes args(0).. which is not available in an incoming jsobject
      strresult4 must beEqualTo("nope!")

    }

    "  convert various values to integers" in {

      val json = Json.parse(s"""{
            "<(toInt)>" : 555
        }""").as[JsObject]
      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val strresult = param.getResult.getOrElse(JsNumber(9999)).as[Int]
      strresult must beEqualTo(555)

      val json1 = Json.parse(s"""{
            "<(toInt)>" : 554.6666
        }""").as[JsObject]
      val param1 = new SectionParameter(json1)
      param1.evaluate(null, Map[String, JsValue]())
      val strresult1 = param1.getResult.getOrElse(JsNumber(9999)).as[Int]
      strresult1 must beEqualTo(554)

      val json2 = Json.parse(s"""{
            "<(toInt)>" : "556.7777"
        }""").as[JsObject]
      val param2 = new SectionParameter(json2)
      param2.evaluate(null, Map[String, JsValue]())
      val strresult2 = param2.getResult.getOrElse(JsNumber(9999)).as[Int]
      strresult2 must beEqualTo(556)

      val json3 = Json.parse(s"""{
            "<(toInt)>" : [ "a", 6, ["b", 4, false]]
        }""").as[JsObject]
      val param3 = new SectionParameter(json3)
      param3.evaluate(null, Map[String, JsValue]())
      val strresult3 = param3.getResult.getOrElse(JsNumber(9999)).as[Int]
      // 9999 is expected.. we have no strategy for converting an array to an int
      strresult3 must beEqualTo(9999)

    }

    "  convert various values to numbers" in {

      val json = Json.parse(s"""{
            "<(tonumber)>" : 555
        }""").as[JsObject]
      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      val strresult = param.getResult.getOrElse(JsNumber(9999)).as[Double]
      strresult must beEqualTo(555)

      val json1 = Json.parse(s"""{
            "<(toNumber)>" : 554.6666
        }""").as[JsObject]
      val param1 = new SectionParameter(json1)
      param1.evaluate(null, Map[String, JsValue]())
      val strresult1 = param1.getResult.getOrElse(JsNumber(9999)).as[Double]
      strresult1 must beEqualTo(554.6666)

      val json2 = Json.parse(s"""{
            "<(toNumber)>" : "556.7777"
        }""").as[JsObject]
      val param2 = new SectionParameter(json2)
      param2.evaluate(null, Map[String, JsValue]())
      val strresult2 = param2.getResult.getOrElse(JsNumber(9999)).as[Double]
      strresult2 must beEqualTo(556.7777)

      val json3 = Json.parse(s"""{
            "<(toNumber)>" : [ "a", 6, ["b", 4, false]]
        }""").as[JsObject]
      val param3 = new SectionParameter(json3)
      param3.evaluate(null, Map[String, JsValue]())
      val strresult3 = param3.getResult.getOrElse(JsNumber(9999)).as[Double]
      // 9999 is expected.. we have no strategy for converting an array to an int
      strresult3 must beEqualTo(9999)

    }

    "  clearly complain about non-existent operator" in {

      val json = Json.parse(s"""{
            "<(doesNotEvenExistNope)>" : 555
        }""").as[JsObject]
      val param = new SectionParameter(json)
      param.evaluate(null, Map[String, JsValue]())
      Json.stringify(param.toJson.as[JsObject].value("timeline").as[JsArray].value(0)).contains("""Unexpected Exception! There is no operator '<(doesNotEvenExistNope)>' available!""")

    }

  }
}
