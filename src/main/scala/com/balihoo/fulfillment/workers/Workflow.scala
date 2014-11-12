package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._
import org.joda.time._
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMapi, MutableList}
import scala.collection.parallel._
import scala.concurrent.forkjoin.ForkJoinPool

import com.amazonaws.services.simpleworkflow.model.{
  StartWorkflowExecutionRequest,
  TaskList,
  WorkflowType
}

abstract class AbstractWorkflowGenerator extends FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
        new ActivityParameter("template", "string", "the template for the workflows", true),
        new ActivityParameter("substitutions", "string", "substitution data for workflows", false)
    ), new ActivityResult("JSON", "list of workflow ids"))
  }

  override def handleTask(params: ActivityParameters) = {

    withTaskHandling {
      val template = params("template")
      val results:MutableList[String] = MutableList[String]()
      val tags = List[String]("generated")

      def submitAndRecord(ffdoc:String) = {
        results += submitTask(ffdoc, tags)
      }

      if (params.has("substitutions")) {
        val submap = jsonStringToSubTable(params("substitutions"))
        multipleSubstitute(template, submap, submitAndRecord)
      } else {
        results += submitTask(template, tags)
      }

      results.mkString(",")
    }
  }

  def jsonStringToSubTable(input:String): Map[String,List[String]] = {
    val subMapJson = Json.parse(input)

    subMapJson.validate[Map[String,List[String]]] match {
      case JsSuccess(submap, _) => submap
      case JsError(e) => throw new Exception("unable to parse substitutions map: ${e.toString}")
    }
  }

  def multipleSubstitute(template:String, subTable:Map[String,List[String]], f:(String => Unit)) = {
    val parallellism = new ForkJoinTaskSupport(new ForkJoinPool(10))

    def iterateSubs(
      subTable:Map[String,List[String]],
      subs:Map[String,String] = Map[String,String]()
    ): Unit = {
      if (subTable.isEmpty) {
        var ffdoc:String = template
        for ((key,value) <- subs) {
          ffdoc = ffdoc.replaceAllLiterally(s"${key}",value)
        }
        synchronized { f(ffdoc) }
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

  def submitTask(input:String, tags: List[String]) = {
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
    swfAdapter.client.startWorkflowExecution(executionRequest).getRunId
    id
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
