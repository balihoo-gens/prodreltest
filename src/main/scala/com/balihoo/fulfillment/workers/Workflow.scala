package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._
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

  override def additionalSchemaValues = Map(
    "minProperties" -> Json.toJson(1),
    "patternProperties" -> Json.obj(
      "[\r\n.]+" -> Json.obj(
        "type" -> "array",
        "minItems" -> 1,
        "items" -> Json.obj(
          "type" -> "string"
        )
      )
    )
    //"additionalProperties" -> Json.toJson(false)
  )

  def parseValue(js:JsValue):SubTable = {
    js.validate[SubTable] match {
      case JsSuccess(submap, _) => submap
      case JsError(e) => throw new Exception("unable to parse substitutions map: ${e.toString}")
    }
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

/**
 * Worker to generate one or more workflows
 */
abstract class AbstractWorkflowGenerator
  extends FulfillmentWorker
  with SubTableTypeDef {

  this: LoggingWorkflowAdapter =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ObjectParameter("template", "the template for the workflows"),
      new SubTableActivityParameter("substitutions", "substitution data for workflows", required=false),
      new StringsParameter("tags", "tags to put on the resulting workflows", required=false)
    ), new ArrayResultType("list of workflow ids", elementType =
      new ObjectResultType("workflow id", properties = Map(
        "workflowId" -> new StringResultType(""),
        "runId" -> new StringResultType("")
      ))
    ))
  }

  trait SubProcessor {
    def process(subs:Map[String,String])
  }

  class WorkFlowCreator(template: String, tags: List[String]) extends SubProcessor {
    val results = ArrayBuffer[WorkflowExecutionIds]()

    /** craft a log message that shows what was replaced, but avoid putting
      * in strings longer that 10 characters
      */
    def abbreviateSubs(subs: Map[String,String]):String = {
      val abbr = Abbreviator.ellipsis _
      subs.foldLeft("substituted:") {
        (s,kv) => s"$s (${abbr(kv._1,10)} -> ${abbr(kv._2,10)})"
      }
    }

    /** substitutes the values and submits the workflow */
    override def process(subs: Map[String,String]) = {
      var ffdoc: String = template
      var fftags: List[String] = tags

      //perform the replacements
      for ((key,value) <- subs) {
        ffdoc = ffdoc.replaceAllLiterally(key,value)
        fftags = for (tag <- fftags) yield tag.replaceAllLiterally(key,value)
      }
      splog.info(abbreviateSubs(subs))

      val result = submitTask(ffdoc, fftags)
      results += result
      splog.info(s"completed result ${results.size}: $result")
    }

    override def toString: String = results.mkString("[", ",", "]")
  }


  override def handleTask(params: ActivityArgs):ActivityResult = {

    val template = params[ActivityArgs]("template").input
    val tags = Try(params[List[String]]("tags")) getOrElse List[String]()
    val workflowCreator = new WorkFlowCreator(template, tags)

    if (params.has("substitutions")) {
      val subTable = params[SubTable]("substitutions")
      multipleSubstitute(subTable, workflowCreator)
    } else {
      submitTask(template, tags)
    }

    getSpecification.createResult(for(result <- workflowCreator.results) yield result.toJson)
  }


  /**
   * recursively and in parallel generate the set of substitutions from
   * the table, and call the 'process' function on the passed in object
   * @param subtable a table containing all the desired substitutions
   * @param processor object on which 'process' with each set of substitutions
   */
  def multipleSubstitute(subTable:SubTable, processor:SubProcessor) = {
    //limit parallel processing to 10 threads
    val parallellism = new ForkJoinTaskSupport(new ForkJoinPool(10))
    //create an object specifically for use by this function
    object mutex

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
        //synchronize the call to user-supplied function f. The caller
        //cannot be expected to provide a thread safe function
        mutex.synchronized {
          processor.process(subs)
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
