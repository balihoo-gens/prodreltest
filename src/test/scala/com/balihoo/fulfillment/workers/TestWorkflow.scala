package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import org.junit.runner.RunWith
import org.specs2.matcher._
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList
import play.api.libs.json._

@RunWith(classOf[JUnitRunner])
class TestWorkflow extends Specification with JsonMatchers with Mockito {

  /**
   * A testable version of Workflow with mocks
   * @param webServer
   */
  class TestableWorkflowGenerator
    extends AbstractWorkflowGenerator
    with LoggingWorkflowAdapterTestImpl {

    // This will change after the task completes successfully.
    var result: Option[String] = None

    // Override this method to avoid actually submitting, but record the input instead
    override def submitTask(input:String, tags: List[String]) = {
      new WorkflowExecutionIds(input, tags.mkString(","))
    }
  }

  "WorkflowGenerator" should {
    "validate substitution input" in {

      val sbparam = new SubTableActivityParameter("", "")
      val subTable = sbparam.parseValue(Json.parse("""
        |{
        |  "key1" : [ "val1", "val2", "val3" ],
        |  "key2" : [ "val1", "val2", "val3" ]
        |}
        """.stripMargin
      ))

      subTable must haveKeys("key1", "key2")
      subTable("key1") must have size(3)
      subTable("key2") must contain("val1", "val2", "val3")
    }

    "produce expected string subsitutions" in {
      val wfgen = new TestableWorkflowGenerator()
      val input = "prefix item of postfix"
      val subTable = Map(
        "prefix" -> List(
          "Ultimate", "Bloody", "Crooked", "Hallowed", "Magnificent",
          "Heavy", "Jagged", "Grand", "Shiny", "Rusty"
        ),
        "item" -> List(
          "Chainsaw", "Towel", "Ping-Pong Ball", "Longsword", "Scissors", "Dagger", "Blade",
          "Spoon", "Fork", "Coat", "Mirror", "Cauldron", "Pouch", "Boots", "Shoes", "Pants",
          "Locket", "Ring", "Amulet", "Potion", "Fish", "Teapot", "Hood", "Crown", "Towel"
        ),
        "postfix" -> List(
          "Destruction", "Twilight", "Dread", "Terror", "Mutual Understanding", "Spite", "Immobility",
          "Mediocrity", "Anger", "the Occult", "the Captain", "the Warrior","the Grue"
        )
      )

      object proc extends wfgen.SubProcessor {
        val results = MutableList[String]()
        override def process(subs: Map[String,String]) = {
          var s = input
          for ((key,value) <- subs) {
            s = s.replaceAllLiterally(key, value)
          }
          results += s
        }
      }
      wfgen.multipleSubstitute(subTable, proc)
      proc.results must contain("Rusty Towel of Mutual Understanding")
      proc.results must contain("Jagged Pants of Anger")
      proc.results must contain("Hallowed Fish of the Occult")
      val total = subTable.keys.foldLeft(1) { (t,key) => t*subTable(key).size }
      proc.results must have size(total)
    }

    "produce valid json" in {
      val wfgen = new TestableWorkflowGenerator()
      val replaceableObject = Json.obj("replace" -> "me")
      val replacingObject1 = Json.obj("replaced" -> "it")
      val replacingObject2 = Json.obj("this" -> Json.arr("is", "not", "even", "the", "same", "structure", true, 1, JsNull))

      val jsonInput = Json.obj(
        "template" -> Json.obj(
          "somekey" -> "#LOC#",
          "anotherkey" -> replaceableObject
        ),
        "substitutions" -> Json.obj(
          "\"#LOC#\"" -> Json.arr(
            """ { "this": ["is", "json"], "and": ["so", "is", "this"] } """,
            """ "this is just a string" """
          ),
          Json.stringify(replaceableObject) -> Json.arr(
            Json.stringify(replacingObject1),
            Json.stringify(replacingObject2)
          )
        )
      )
      val input = Json.stringify(jsonInput)

      val result = wfgen.handleTask(wfgen.getSpecification.getArgs(input))
//      println(result.serialize())
      result.serialize() match {
        case s:String =>
          val results = Json.parse(s).as[List[JsObject]]
          results must have size(4)
          for (result <- results) {
            //this just contains the workflow doc so we can
            //validate it; would contain the wfid in reality
            val wfinput = (result \ "workflowId").as[String]
            wfinput must not */("#LOC#")
            Json.parse(wfinput) must not throwA(new Exception)
          }
          success
        case _ => failure
      }
      success
    }

    "replace values in tags" in {
      val wfgen = new TestableWorkflowGenerator()
      val jsonInput = Json.obj(
        "template" -> Json.obj(
          "somekey" -> "#LOC#"
        ),
        "substitutions" -> Json.obj(
          "\"#LOC#\"" -> Json.arr("1234")
        ),
        "tags" -> Json.arr("#LOC#", "NOLOC", "\"#LOC#\"")
      )
      val input = Json.stringify(jsonInput)
      val result = wfgen.handleTask(wfgen.getSpecification.getArgs(input))
      result.serialize() match {
        case s:String =>
          val results = Json.parse(s).as[List[JsObject]]
          results must have size(1)
          for (result <- results) {
            val wfinput = (result \ "runId").as[String]
            wfinput must beEqualTo("#LOC#,NOLOC,1234")
          }
        case _ => failure
      }
      success
    }

    "abbreviate a sublist" in {
      val wfgen = new TestableWorkflowGenerator()
      val wfc = new wfgen.WorkFlowCreator("", List[String]())

      val subList = Map[String,String](
        "123456789ABCDEF" -> "short",
        "short" -> "123456789ABCDEF"
      )
      wfc.abbreviateSubs(subList) must beEqualTo("substituted: (1234567... -> short) (short -> 1234567...)")
    }
  }
}
