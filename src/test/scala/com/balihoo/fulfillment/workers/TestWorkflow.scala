package com.balihoo.fulfillment.workers

import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import org.junit.runner.RunWith
import org.specs2.matcher._
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection.JavaConversions._
import scala.collection.mutable.{Map => MutableMapi, MutableList}

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

    // Override this method to simplify testing and to avoid swallowing exceptions.
    override def withTaskHandling(code: => String): Unit = result = Some(code)

    // Override this method to avoid actually submitting, but record the input instead
    override def submitTask(input:String, tags: List[String]) = {
      input
    }
  }

  "WorkflowGenerator" should {
    "validate substitution input" in {

      val wfgen = new TestableWorkflowGenerator()
      val subTable = wfgen.jsonStringToSubTable("""
        |{
        |  "key1" : [ "val1", "val2", "val3" ],
        |  "key2" : [ "val1", "val2", "val3" ]
        |}
        """.stripMargin
      )

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

      val results = MutableList[String]()
      wfgen.multipleSubstitute(input, subTable, (s:String) => results += s)
      results must contain("Rusty Towel of Mediocrity")
      results must contain("Jagged Pants of Anger")
      results must contain("Rusty Locket of the Occult")
    }

    "produce valid json" in {
      val wfgen = new TestableWorkflowGenerator()
      val params = new ActivityParameters(Map(
        //this template is NOT valid json
        "template" -> """{ "missingquote: { "value" : missing end curly bracket }""",
        //sub in partial json to complete the results
        "substitutions" -> """
        |{
        |  "missingquote" : ["section one\"", "section two\""],
        |  "missing end curly bracket" : [ "\"value one\" }", "\"value two\" }" ]
        |}
        """.stripMargin
      ))
      //
      wfgen.handleTask(params)
      wfgen.result match {
        case Some(s) =>
          val results = s.split(",")
          for (result <- results) {
            result must */("value (one|two)".r)
          }
          results must have size(4)
        case _ => failure
      }
      success
    }
  }
}
