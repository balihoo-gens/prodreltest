package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._
import org.joda.time._
import scala.collection.JavaConverters._

import com.amazonaws.services.simpleworkflow.model.{
  StartWorkflowExecutionRequest,
  TaskList,
  WorkflowType
}

abstract class AbstractBenchmark extends FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
        new ActivityParameter("token", "string", "some identifier", false),
        new ActivityParameter("count", "int", "How many workflows to iterate", false),
        new ActivityParameter("multiply", "int", "Exponential multiplication factor at each iteration", false)
    ), new ActivityResult("JSON", "completed time and token"))
  }

  override def handleTask(params: ActivityParameters) = {
    val count = params.getOrElse("count", 1)
    val multiply = params.getOrElse("multiply", 1)
    val token = params.getOrElse("token", uuid)

    if (count > 0) {
      val newcount = count -1
      for (i <- 1 to multiply) {
        val input = createInput(newcount, i, multiply, token)
        val runid = submitTask(input, List(s"Token: $token"))
        splog.info(s"submitted $runid: $count, ($i of $multiply), token: $token")
      }
    }

    val now:DateTime = new DateTime(DateTimeZone.UTC)
    completeTask(s"""{ "time": "${now.toString}", "token": "$token" }""")
  }

  def createInput(count:Int, instance:Int, multiply:Int, token:String):String = {
    val jsobj = Json.obj(
      "benchmark" -> Json.obj(
        "action" -> Json.obj(
          "name" -> name.toString,
          "version" -> version.toString
        ),
        "params" -> Json.obj(
          "token" -> token,
          "count" -> count.toString,
          "multiply" -> multiply.toString
        ),
        "status" -> "READY",
        "essential" -> true
      )
    )
    Json.stringify(jsobj)
  }

  def uuid = java.util.UUID.randomUUID.toString

  def submitTask(input:String, tags: List[String]) = {
    val executionRequest = new StartWorkflowExecutionRequest()
    executionRequest.setDomain(swfAdapter.config.getString("domain"))
    executionRequest.setWorkflowId(s"benchmark_$uuid")
    executionRequest.setInput(input)
    executionRequest.setTagList(tags.asJavaCollection)
    val workflowName = new SWFName(swfAdapter.config.getString("workflowName"))
    val workflowVersion = new SWFVersion(swfAdapter.config.getString("workflowVersion"))
    val taskListName = new SWFName(workflowName + workflowVersion)
    executionRequest.setWorkflowType(new WorkflowType().withName(workflowName).withVersion(workflowVersion))
    executionRequest.setTaskList(new TaskList().withName(taskListName))
    swfAdapter.client.startWorkflowExecution(executionRequest).getRunId
  }
}


class Benchmark(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractBenchmark
  with LoggingWorkflowAdapterImpl {
}

object benchmark extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new Benchmark(cfg, splog)
  }
}
