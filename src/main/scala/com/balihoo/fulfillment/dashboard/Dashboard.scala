package com.balihoo.fulfillment.dashboard

import java.io.File
import java.util.Date

import com.balihoo.fulfillment.deciders.{DecisionGenerator, CategorizedSections, FulfillmentSection, SectionMap}
import com.balihoo.fulfillment.workers.{UTCFormatter, FulfillmentWorkerTable, FulfillmentWorkerEntry}
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
      Json.parse(input)
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
      Json.toJson(Map(
        "workflowId" -> Json.toJson(info.getExecution.getWorkflowId),
        "runId" -> Json.toJson(info.getExecution.getRunId),
        "closeStatus" -> Json.toJson(info.getCloseStatus),
        "closeTimestamp" -> Json.toJson(if(info.getCloseTimestamp != null) info.getCloseTimestamp.toString else "--"),
        "startTimestamp" -> Json.toJson(info.getStartTimestamp.toString),
        "tagList" -> Json.toJson(info.getTagList.asScala)
      ))
    }

    /**
     * This function is still a bummer.
     * @param event: HistoryEvent
     * @return String
     */
    def _getEventAttribs(event:HistoryEvent):String = {

      for(f <- List(event.getWorkflowExecutionStartedEventAttributes _
        ,event.getWorkflowExecutionCompletedEventAttributes _
        ,event.getCompleteWorkflowExecutionFailedEventAttributes _
        ,event.getWorkflowExecutionFailedEventAttributes _
        ,event.getFailWorkflowExecutionFailedEventAttributes _
        ,event.getWorkflowExecutionTimedOutEventAttributes _
        ,event.getWorkflowExecutionCanceledEventAttributes _
        ,event.getCancelWorkflowExecutionFailedEventAttributes _
        ,event.getWorkflowExecutionContinuedAsNewEventAttributes _
        ,event.getContinueAsNewWorkflowExecutionFailedEventAttributes _
        ,event.getWorkflowExecutionTerminatedEventAttributes _
        ,event.getWorkflowExecutionCancelRequestedEventAttributes _
        ,event.getDecisionTaskScheduledEventAttributes _
        ,event.getDecisionTaskStartedEventAttributes _
        ,event.getDecisionTaskCompletedEventAttributes _
        ,event.getDecisionTaskTimedOutEventAttributes _
        ,event.getActivityTaskScheduledEventAttributes _
        ,event.getActivityTaskStartedEventAttributes _
        ,event.getActivityTaskCompletedEventAttributes _
        ,event.getActivityTaskFailedEventAttributes _
        ,event.getActivityTaskTimedOutEventAttributes _
        ,event.getActivityTaskCanceledEventAttributes _
        ,event.getActivityTaskCancelRequestedEventAttributes _
        ,event.getWorkflowExecutionSignaledEventAttributes _
        ,event.getMarkerRecordedEventAttributes _
        ,event.getRecordMarkerFailedEventAttributes _
        ,event.getTimerStartedEventAttributes _
        ,event.getTimerFiredEventAttributes _
        ,event.getTimerCanceledEventAttributes _
        ,event.getStartChildWorkflowExecutionInitiatedEventAttributes _
        ,event.getChildWorkflowExecutionStartedEventAttributes _
        ,event.getChildWorkflowExecutionCompletedEventAttributes _
        ,event.getChildWorkflowExecutionFailedEventAttributes _
        ,event.getChildWorkflowExecutionTimedOutEventAttributes _
        ,event.getChildWorkflowExecutionCanceledEventAttributes _
        ,event.getChildWorkflowExecutionTerminatedEventAttributes _
        ,event.getSignalExternalWorkflowExecutionInitiatedEventAttributes _
        ,event.getExternalWorkflowExecutionSignaledEventAttributes _
        ,event.getSignalExternalWorkflowExecutionFailedEventAttributes _
        ,event.getExternalWorkflowExecutionCancelRequestedEventAttributes _
        ,event.getRequestCancelExternalWorkflowExecutionInitiatedEventAttributes _
        ,event.getRequestCancelExternalWorkflowExecutionFailedEventAttributes _
        ,event.getScheduleActivityTaskFailedEventAttributes _
        ,event.getRequestCancelActivityTaskFailedEventAttributes _
        ,event.getStartTimerFailedEventAttributes _
        ,event.getCancelTimerFailedEventAttributes _
        ,event.getStartChildWorkflowExecutionFailedEventAttributes _
      )) {
        f() match {
          case o:AnyRef => return o.toString
          case _ =>
        }
      }
      "Unknown event type!"
    }

    def executionHistory(oldest:Date, latest:Date):List[JsValue] = {
      val filter = new ExecutionTimeFilter
      filter.setOldestDate(oldest)
      filter.setLatestDate(latest)

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

      val top = collection.mutable.Map[String, JsValue]()

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
      val sectionMap = new SectionMap(events)
      val categorized = new CategorizedSections(sectionMap)
      new DecisionGenerator(categorized, sectionMap).makeDecisions()

      val sections = collection.mutable.Map[String, JsValue]()
      for((name, section:FulfillmentSection) <- sectionMap.nameToSection) {
        sections(name) = section.toJson
      }

      val jtimeline = Json.toJson(for(entry <- sectionMap.timeline.events) yield entry.toJson)
      val executionHistory = Json.toJson(for(event:HistoryEvent <- collectionAsScalaIterable(events)) yield Json.toJson(Map(
        "type" -> Json.toJson(event.getEventType),
        "id" -> Json.toJson(event.getEventId.toString),
        "timestamp" -> Json.toJson(UTCFormatter.format(event.getEventTimestamp)),
        "attributes" -> Json.toJson(_getEventAttribs(event))
      )))

      top("timeline") = jtimeline
      top("sections") = Json.toJson(sections.toMap)
      top("workflowId") = Json.toJson(workflowId)
      top("runId") = Json.toJson(runId)
      top("input") = Json.toJson(events.get(0).getWorkflowExecutionStartedEventAttributes.getInput)
      top("resolution") = Json.toJson(sectionMap.resolution)
      top("history") = executionHistory

      top.toMap
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
          UTCFormatter.dateFormat.parse(rsq.getRequiredParameter("startDate")),
          UTCFormatter.dateFormat.parse(rsq.getRequiredParameter("endDate"))
      ))))
  })

  get("/workflow/detail", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowInspector.workflowSections(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
      ))))
  })

  get("/workflow/update", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowUpdater.update(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
        ,rsq.getRequiredParameter("input")
      ))))
  })

  get("/workflow/environment", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowInspector.environment())))
  })

  get("/workflow/initiate", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(workflowInitiator.initate(
        rsq.getRequiredParameter("id"),
        rsq.getRequiredParameter("input"),
        rsq.getOptionalParameter("tags", "").split(",").toList
      ))))
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
