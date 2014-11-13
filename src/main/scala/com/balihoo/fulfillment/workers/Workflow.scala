package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._
import org.joda.time._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel._
import scala.concurrent.forkjoin.ForkJoinPool

import com.amazonaws.services.simpleworkflow.model.{
  StartWorkflowExecutionRequest,
  TaskList,
  WorkflowType
}

class SubTableActivityParameter(
  override val name:String,
  override val description:String,
  override val required:Boolean = true
) extends ActivityParameter(name, description, required) {

  type SubTable = Map[String,List[String]]

  def jsonType = "object"

  def parseValue(js:JsValue):SubTable = {
    js.validate[SubTable] match {
      case JsSuccess(submap, _) => submap
      case JsError(e) => throw new Exception("unable to parse substitutions map: ${e.toString}")
    }
  }

  override def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> description,
      "minProperties" -> 1,
      "patternProperties" -> Json.obj(
        "[\r\n.]+" -> Json.obj(
          "type" -> "array",
          "minItems" -> 1,
          "items" -> Json.obj(
            "type" -> "string"
          )
        )
      ),
      "additionalProperties" -> false
    )
  }
}

abstract class AbstractWorkflowGenerator extends FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  //worker specific type to store substitution tables
  type SubTable = Map[String,List[String]]

  //worker specific type to store swf execution ids
  case class WorkFlowExecutionIds(workflowId:String, runId:String)

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
        new StringActivityParameter("template", "the template for the workflows", true),
        new SubTableActivityParameter("substitutions", "substitution data for workflows", false)
        new StringsActivityParameter("tags", "tags to put on the resulting workflows", false)
    ), new ActivityResult("JSON", "list of workflow ids"))
  }

  override def handleTask(params: ActivityParameters) = {

    withTaskHandling {
      val template = params[String]("template")
      val results = ArrayBuffer[String]()
      val tags = getOrElse(params[List[String]]("tags"), List[String]())
      val subTable = params[SubTable]("substitutions")

      def submitAndRecord(ffdoc:String) = {
        results += submitTask(ffdoc, tags)
        splog.info(s"")
      }

      if (params.has("substitutions")) {
        //pass the 
        multipleSubstitute(template, subTable, submitAndRecord _)
      } else {
        submitAndRecord(template)
      }

      results.mkString(",")
    }
  }

  def multipleSubstitute(template:String, subTable:SubTable, f:(String => Unit)) = {
    val parallellism = new ForkJoinTaskSupport(new ForkJoinPool(10))
    object mutex

    //strings can get long, abbreviate with dots
    def abbr(s:String, n:Int) = {
      if (s.size > n) s"${s.take(n)}..." else s
    }

    def iterateSubs(
      subTable:SubTable,
      subs:Map[String,String] = Map[String,String]()
    ): Unit = {
      if (subTable.isEmpty) {
        var ffdoc:String = template
        for ((key,value) <- subs) {
          ffdoc = ffdoc.replaceAllLiterally(s"${key}",value)
        }

        //craft a log message that shows what was replaced, but avoid putting
        //  in strings longer that 10 characters
        val logmsg = m.foldLeft("substituted: ") {
          (s,kv) => s"""$s ("${abbr(kv._1,10)}" -> "${abbr(kv._2,10)}")"""
        }

        //
        mutex.synchronized {
          splog.info(logmsg)
          f(ffdoc)
        }
      } else {
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

  def uuid = java.util.UUID.randomUUID.toString

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
