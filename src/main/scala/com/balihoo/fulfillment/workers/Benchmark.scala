package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._
import org.joda.time._
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}

import com.amazonaws.services.simpleworkflow.model.{
  StartWorkflowExecutionRequest,
  TaskList,
  WorkflowType
}

abstract class AbstractBenchmark extends FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
        new ActivityParameter("token", "string", "some identifier to tie the chain together", false),
        new ActivityParameter("maxcount", "int", "How many workflows to iterate", false),
        new ActivityParameter("multiply", "int", "Exponential multiplication factor at each iteration", false),
        new ActivityParameter("count", "int", "system: How manieth workflow this is", false),
        new ActivityParameter("submit_time", "int", "system: the time this workflow was submitted", false),
        new ActivityParameter("avg_duration", "int", "system: the average time between submittal and handling for workflows in this chain", false)
    ), new ActivityResult("JSON", "completed time and token"))
  }

  override def handleTask(params: ActivityParameters) = {
    val timeReceived = new DateTime(DateTimeZone.UTC)
    val countMax = params.getOrElse("maxcount", 1)
    val countPrevious = params.getOrElse("count", 0)
    val multiply = params.getOrElse("multiply", 1)
    val token = params.getOrElse("token", uuid)

    val count = countPrevious + 1

    val durationLast:Option[Duration] = params.get("submit_time") match {
      case Some(timeSubmittedString) =>
        try {
          val timeSubmitted = DateTime.parse(timeSubmittedString)
          Some(new Duration(timeReceived, timeSubmitted))
        } catch {
          case e:Exception =>
            None
        }
      case None => None
    }


    val durationAvg:Option[Duration] = durationLast match {
      case Some(duration) =>
        params.get("avg_duration") match {
          case Some(durationAvgPrevious) =>
            val prevMillis = durationAvgPrevious.toLong
            val curMillis = timeReceived.getMillis
            val avgMillis = ((countPrevious * prevMillis) + curMillis) / count
            Some(new Duration(avgMillis))
          case None =>
            durationLast
        }
      case None =>
        None
    }

    def createInput(instance:Int):String = {
      val params = MutableMap(
        "token" -> token,
        "count" -> count.toString,
        "maxcount" -> countMax.toString,
        "multiply" -> multiply.toString,
        "submit_time" -> new DateTime(DateTimeZone.UTC).toString
      )

      if (!durationAvg.isEmpty) {
        val durationMillis = durationAvg.get.getMillis
        params("avg_duration") = durationMillis.toString
      }

      val jsobj = Json.obj(
        "benchmark" -> Json.obj(
          "action" -> Json.obj(
            "name" -> name.toString,
            "version" -> version.toString
          ),
          "params" -> Json.toJson(params.toMap),
          "status" -> "READY",
          "essential" -> true
        )
      )
      Json.stringify(jsobj)
    }

    if (count < countMax) {
      for (i <- 1 to multiply) {
        val input = createInput(i)
        val runid = submitTask(input, List(s"Token: $token"))
        splog.info(s"submitted $runid: $count, ($i of $multiply), token: $token")
      }
    }

    val result = MutableMap(
      "token" -> token,
      "count" -> countPrevious.toString
    )

    def addDuration(name:String, duration:Option[Duration]) = {
      if (!duration.isEmpty) {
        val durationMillis = duration.get.getMillis
        result(name) = durationMillis.toString
      }
    }

    addDuration("Duration", durationLast)
    addDuration("Average Duration", durationAvg)

    completeTask(Json.stringify(Json.toJson(result.toMap)))
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
