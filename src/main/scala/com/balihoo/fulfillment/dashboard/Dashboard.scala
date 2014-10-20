package com.balihoo.fulfillment.dashboard

import java.io.File
import java.util

import com.balihoo.fulfillment.SWFHistoryConvertor
import com.balihoo.fulfillment.deciders._
import com.balihoo.fulfillment.workers.{UTCFormatter, FulfillmentWorkerTable, FulfillmentWorkerEntry}
import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.collection.convert.wrapAsScala._

import javax.servlet.http.HttpServletResponse

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext

trait WorkflowInitiatorComponent {
  def workflowInitiator: WorkflowInitiator with SWFAdapterComponent

  class AbstractWorkflowInitiator {
    this: SWFAdapterComponent =>

    def initate(id:String, input:String, tags:List[String]): String = {

      // First validate the input as best as we can..
      val fulfillmentInput = Json.parse(input).as[JsObject]
      val fulfillmentSections = new Fulfillment(List())
      fulfillmentSections.initializeWithInput(fulfillmentInput, DateTime.now())

      val errors = new util.ArrayList[String]()
      for((name, section) <- fulfillmentSections.nameToSection) {
        for(event <- section.timeline.events) {
          if(event.eventType == TimelineEventType.ERROR) {
            errors.add(s"$name: ${event.message}")
          }
        }
      }

      if(errors.size > 0) {
        throw new Exception(errors.mkString("Input has errors!", ",\n\t", ""))
      }

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
    }
  }

  class WorkflowInitiator(swf: SWFAdapter) extends AbstractWorkflowInitiator with SWFAdapterComponent {
    def swfAdapter = swf
  }
}

trait WorkflowUpdaterComponent {
  def workflowUpdater: WorkflowUpdater with SWFAdapterComponent

  class AbstractWorkflowUpdater {
    this: SWFAdapterComponent =>

    def update(runId:String, workflowId:String, input:String) = {
      val req = new SignalWorkflowExecutionRequest()
      req.setDomain(swfAdapter.config.getString("domain"))
      req.setRunId(runId)
      req.setWorkflowId(workflowId)
      req.setSignalName("sectionUpdates")
      req.setInput(input)

      swfAdapter.client.signalWorkflowExecution(req)

      "success"
    }

    def cancel(runId:String, workflowId:String) = {
      val req = new RequestCancelWorkflowExecutionRequest
      req.setDomain(swfAdapter.config.getString("domain"))
      req.setRunId(runId)
      req.setWorkflowId(workflowId)
      swfAdapter.client.requestCancelWorkflowExecution(req)

      "success"
    }

    def terminate(runId:String, workflowId:String, reason:String, details:String) = {
      val treq = new TerminateWorkflowExecutionRequest
      treq.setDomain(swfAdapter.config.getString("domain"))
      treq.setRunId(runId)
      treq.setWorkflowId(workflowId)
      treq.setReason(reason)
      treq.setDetails(details)
//      treq.setChildPolicy(ChildPolicy.fromValue())
      swfAdapter.client.terminateWorkflowExecution(treq)

      "success"
    }
  }

  class WorkflowUpdater(swf: SWFAdapter) extends AbstractWorkflowUpdater with SWFAdapterComponent {
    def swfAdapter = swf
  }
}


trait WorkflowInspectorComponent {
  def workflowInspector: WorkflowInspector with SWFAdapterComponent

  class AbstractWorkflowInspector {
    this: SWFAdapterComponent =>

    def infoToJson(info: WorkflowExecutionInfo): JsValue = {
      Json.obj(
        "workflowId" -> info.getExecution.getWorkflowId,
        "runId" -> info.getExecution.getRunId,
        "closeStatus" -> info.getCloseStatus,
        "closeTimestamp" -> (Option(info.getCloseTimestamp) match {
            case Some(ts) => UTCFormatter.format(ts)
            case None => "--" }),
        "startTimestamp" -> UTCFormatter.format(info.getStartTimestamp),
        "tagList" -> info.getTagList.asScala
      )
    }

    def executionHistory(oldest:DateTime, latest:DateTime):List[JsValue] = {
      val filter = new ExecutionTimeFilter
      filter.setOldestDate(oldest.toDate)
      filter.setLatestDate(latest.toDate)

      val infos = new collection.mutable.MutableList[JsValue]()
      val oreq = new ListOpenWorkflowExecutionsRequest()
      oreq.setDomain(swfAdapter.config.getString("domain"))
      oreq.setStartTimeFilter(filter)
      val open = swfAdapter.client.listOpenWorkflowExecutions(oreq)

      for(info:WorkflowExecutionInfo <- open.getExecutionInfos.asScala) {
        infos += infoToJson(info)
      }

      val creq = new ListClosedWorkflowExecutionsRequest()
      creq.setDomain(swfAdapter.config.getString("domain"))
      creq.setStartTimeFilter(filter)
      val closed = swfAdapter.client.listClosedWorkflowExecutions(creq)

      for(info:WorkflowExecutionInfo <- closed.getExecutionInfos.asScala) {
        infos += infoToJson(info)
      }

      infos.toList
    }


    def workflowSections(runId:String, workflowId:String):Map[String, JsValue] = {

      val exec = new WorkflowExecution
      exec.setRunId(runId)
      exec.setWorkflowId(workflowId)
      val req = new GetWorkflowExecutionHistoryRequest()
      req.setDomain(swfAdapter.config.getString("domain"))
      req.setExecution(exec)


      val events = new java.util.ArrayList[HistoryEvent]()
      var history = swfAdapter.client.getWorkflowExecutionHistory(req)
      events.addAll(history.getEvents)
      // The results come back in pages.
      while(history.getNextPageToken != null) {
        req.setNextPageToken(history.getNextPageToken)
        history = swfAdapter.client.getWorkflowExecutionHistory(req)
        events.addAll(history.getEvents)
      }
      val historyJson = SWFHistoryConvertor.historyToJson(events)
      val sections = new Fulfillment(SWFHistoryConvertor.jsonToSWFEvents(historyJson))
      new DecisionGenerator(sections).makeDecisions(runOperations=false)

      val sectionsJson = collection.mutable.Map[String, JsValue]()
      for((name, section:FulfillmentSection) <- sections.nameToSection) {
        sectionsJson(name) = section.toJson
      }

      val jtimeline = Json.toJson(for(entry <- sections.timeline.events) yield entry.toJson)

      Map(
        "timeline" -> jtimeline,
        "sections" -> Json.toJson(sectionsJson.toMap),
        "workflowId" -> Json.toJson(workflowId),
        "runId" -> Json.toJson(runId),
        "status" -> Json.toJson(sections.status.toString),
        "history" -> historyJson
      )

    }

    def environment() = {
      Json.toJson(Map(
        "domain" -> Json.toJson(swfAdapter.domain),
        "region" -> Json.toJson(swfAdapter.region.getName)
      ))
    }
  }

  class WorkflowInspector(swf: SWFAdapter) extends AbstractWorkflowInspector with SWFAdapterComponent {
    def swfAdapter = swf
  }
}

class AbstractWorkflowServlet extends RestServlet {
  this: SWFAdapterComponent
    with WorkflowInspectorComponent
    with WorkflowInitiatorComponent
    with WorkflowUpdaterComponent =>

  get("/workflow/history", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowInspector.executionHistory(
          new DateTime(rsq.getRequiredParameter("startDate")),
          new DateTime(rsq.getRequiredParameter("endDate"))
      ))))
  })

  get("/workflow/detail", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowInspector.workflowSections(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
      ))))
  })

  post("/workflow/update", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowUpdater.update(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
        ,rsq.getRequiredParameter("input")
      ))))
  })

  post("/workflow/cancel", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowUpdater.cancel(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
      ))))
  })

  post("/workflow/terminate", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowUpdater.terminate(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
        ,rsq.getOptionalParameter("reason", "no reason specified")
        ,rsq.getOptionalParameter("details", "")
      ))))
  })

  get("/workflow/environment", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowInspector.environment())))
  })

  post("/workflow/initiate", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(Map(
        "runId" -> workflowInitiator.initate(
          rsq.getRequiredParameter("id"),
          rsq.getRequiredParameter("input"),
          rsq.getOptionalParameter("tags", "").split(",").toList
      )))))
  })

}

class WorkflowServlet(swf: SWFAdapter)
  extends AbstractWorkflowServlet
    with WorkflowInspectorComponent
    with WorkflowInitiatorComponent
    with WorkflowUpdaterComponent
    with SWFAdapterComponent {
  private val _inspector = new WorkflowInspector(swf)
  private val _initiator = new WorkflowInitiator(swf)
  private val _updater = new WorkflowUpdater(swf)
  def swfAdapter = swf
  def workflowInspector = _inspector
  def workflowInitiator = _initiator
  def workflowUpdater = _updater
}

class AbstractWorkerServlet extends RestServlet {
  this: DynamoAdapterComponent =>

  val da = dynamoAdapter
  val workerTable = new FulfillmentWorkerTable with DynamoAdapterComponent {
    def dynamoAdapter = da
  }

  get("/worker", (rsq:RestServletQuery) => {

    val workers = workerTable.get()

    val workerMap = collection.mutable.Map[String, JsValue]()
    for(worker:FulfillmentWorkerEntry <- workers) {
      workerMap(worker.getInstance()) = worker.toJson
    }

    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workerMap.toMap)))
  })

}

class WorkerServlet(dyn: DynamoAdapter) extends AbstractWorkerServlet with DynamoAdapterComponent {
  def dynamoAdapter = dyn
}

object dashboard {
  def main(args: Array[String]) {

    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))

    val context = new WebAppContext()
    context setContextPath "/"
    if(isRunningFromJar) {
      context.setWar(context.getClass.getClassLoader.getResource("webapp").toExternalForm)
    } else {
      context.setResourceBase("src/main/webapp")
    }
    context.setWelcomeFiles(Array[String]("index.html"))

    val workerServlet = new WorkerServlet(new DynamoAdapter(cfg))
    val workflowServlet = new WorkflowServlet(new SWFAdapter(cfg))

    context.addServlet(new ServletHolder(workerServlet), "/worker/*")
    context.addServlet(new ServletHolder(workflowServlet), "/workflow/*")

    val server = new Server(cfg.getInt("port"))
    server.setHandler(context)
    server.start()
    server.join()
  }

  def isRunningFromJar:Boolean = {
    new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath).isFile
  }
}
