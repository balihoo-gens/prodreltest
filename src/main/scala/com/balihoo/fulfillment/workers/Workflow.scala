package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._
import org.joda.time._
import scala.util.Try
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel._
import scala.concurrent.forkjoin.ForkJoinPool

import com.amazonaws.services.simpleworkflow.model.{
  StartWorkflowExecutionRequest,
  TaskList,
  WorkflowType
}

//worker specific type to store substitution tables
trait SubTableTypeDef {
  type SubTable = Map[String,List[String]]
}

/**
  * Worker specific Activity Parameter type
  * parses jason of this form
  * {
  *   "key1" : ["value1", "value2" ],
  *   "key2" : ["value3", "value4" ],
  * }
  * into a Map[String, List[String]]
  */
class SubTableActivityParameter(
  override val name:String,
  override val description:String,
  override val required:Boolean = true
) extends ActivityParameter(name, description, required)
  with SubTableTypeDef {

  def jsonType = "object"

  def parseValue(js:JsValue):SubTable = {
    js.validate[SubTable] match {
      case JsSuccess(submap, _) => submap
      case JsError(e) => throw new Exception("unable to parse substitutions map: ${e.toString}")
    }
  }

  //see https://gist.github.com/balihoo-gens/e662179accfdb0f8a2b9
  override def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> description,
      //"minProperties" -> 1,
      "patternProperties" -> Json.obj(
        "[\r\n.]+" -> Json.obj(
          "type" -> "array",
          //"minItems" -> 1,
          "items" -> Json.obj(
            "type" -> "string"
          )
        )
      )
      //"additionalProperties" -> false
    )
  }
}

/** worker specific type to store swf execution ids */
case class WorkflowExecutionIds(workflowId:String, runId:String) {
  override def toString:String = {
    Json.stringify(toJson)
  }

  def toJson:JsObject = {
    Json.obj(
      "workflowId" -> workflowId,
      "runId" -> runId
    )
  }
}

/** json version of a list of workflow execution ids */
class WorkflowIdsActivityResult(override val description:String)
  extends ActivityResult(description) {
  def jsonType = "array"

  override def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> description,
      "items" -> Json.obj(
        "type" -> "object",
        "properties" -> Json.obj(
          "workflowId" -> Json.obj(
            "type" -> "string"
          ),
          "runId" -> Json.obj(
            "type" -> "string"
          )
        )
      )
    )
  }
}

/**
 * Worker to generate one or more workflows
 */
abstract class AbstractWorkflowGenerator
  extends FulfillmentWorker
  with SubTableTypeDef {

  this: LoggingWorkflowAdapter =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
        new StringActivityParameter("template", "the template for the workflows", true),
        new SubTableActivityParameter("substitutions", "substitution data for workflows", false),
        new StringsActivityParameter("tags", "tags to put on the resulting workflows", false)
    ), new WorkflowIdsActivityResult("list of workflow ids"))
  }

  override def handleTask(params: ActivityParameters) = {

    withTaskHandling {
      val template = params[String]("template")
      val results = ArrayBuffer[WorkflowExecutionIds]()
      val tags = Try(params[List[String]]("tags")) getOrElse List[String]()

      def submitAndRecord(ffdoc:String) = {
        val result = submitTask(ffdoc, tags)
        results += result
        splog.info(s"completed result ${results.size}: $result")
      }

      if (params.has("substitutions")) {
        val subTable = params[SubTable]("substitutions")
        multipleSubstitute(template, subTable, submitAndRecord _)
      } else {
        submitAndRecord(template)
      }

      results.mkString("[", ",", "]")
      //Json.toJson(results).stringify
    }
  }


  /**
   * recursively and in parallelly generates strings from the template by replacing
   *   the keys with each of the values in the substitution table, and call function
   *   f on each result. Deliberately does not store all intermediate results.
   * @param template the source to replace values in
   * @param subtable a table containing all the desired substitutions
   * @param f the function to be called with each completed substitution result
   */
  def multipleSubstitute(template:String, subTable:SubTable, f:(String => Unit)) = {
    //limit parallel processing to 10 threads
    val parallellism = new ForkJoinTaskSupport(new ForkJoinPool(10))
    //create an object specifically for use by this function
    object mutex

    /**strings can get long, abbreviate with dots */
    def abbr(s:String, n:Int) = {
      if (s.size > n) s"${s.take(n)}..." else s
    }

    /**
      * recursive function to iterate over n nested loops, where n is
      * the number of entries in the substitution map. This results in
      * in calling function f for each combination of substitutions
      * @param subTable the remaining table of substitutions
      * @param the current map of substitutions
      */
    def iterateSubs(
      subTable:SubTable,
      subs:Map[String,String] = Map[String,String]()
    ): Unit = {
      if (subTable.isEmpty) {
        var ffdoc:String = template

        //perform the replacements
        for ((key,value) <- subs) {
          ffdoc = ffdoc.replaceAllLiterally(s"${key}",value)
        }

        //craft a log message that shows what was replaced, but avoid putting
        //  in strings longer that 10 characters
        val logmsg = subs.foldLeft("substituted: ") {
          (s,kv) => s"""$s ("${abbr(kv._1,10)}" -> "${abbr(kv._2,10)}")"""
        }

        //synchronize the call to user-supplied function f. The caller
        //cannot be expected to provide a thread safe function
        mutex.synchronized {
          splog.info(logmsg)
          f(ffdoc)
        }
      } else {
        //parallelly iterate over each value in the head of the sub table
        //recurse over the tail to produce the next nested loop, while
        //adding the current sub value to the map. when the subTable is empty
        //the most inner loop has been reached and substitution can be done
        val (key, vals) = subTable.head
        val parvals = vals.par
        parvals.tasksupport = parallellism
        vals.par.foreach {
          v => iterateSubs(subTable.tail, subs + (key -> v))
        }
      }
    }
    iterateSubs(subTable)
  }

  /** generate a uuid as a string */
  def uuid = java.util.UUID.randomUUID.toString

  /**
    * submit a SWF workflow
    * @param input a valid fulfillment document (not currently schema validated!)
    * @param tags a list of tags to tag the workflow with
    */
  def submitTask(input:String, tags: List[String]): WorkflowExecutionIds = {
    val id=s"genwf_$uuid"
    val executionRequest = new StartWorkflowExecutionRequest()
    executionRequest.setDomain(swfAdapter.config.getString("domain"))
    executionRequest.setWorkflowId(id)
    executionRequest.setInput(input)
    executionRequest.setTagList(tags.asJavaCollection)
    val workflowName = new SWFName(swfAdapter.config.getString("workflowName"))
    val workflowVersion = new SWFVersion(swfAdapter.config.getString("workflowVersion"))
    val taskListName = new SWFName(workflowName + workflowVersion)
    executionRequest.setWorkflowType(new WorkflowType().withName(workflowName).withVersion(workflowVersion))
    executionRequest.setTaskList(new TaskList().withName(taskListName))
    val runId = swfAdapter.client.startWorkflowExecution(executionRequest).getRunId
    new WorkflowExecutionIds(id, runId)
  }
}


class WorkflowGenerator(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractWorkflowGenerator
  with LoggingWorkflowAdapterImpl {
}

object workflow extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new WorkflowGenerator(cfg, splog)
  }
}