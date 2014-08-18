package com.balihoo.fulfillment.dashboard

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
      executionRequest.setWorkflowType(new WorkflowType().withName("generic").withVersion("1"))
      executionRequest.setTaskList(new TaskList().withName("generic1"))
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
     * This function is a bummer. I don't see any other way.
     * @param event: HistoryEvent
     * @return String
     */
    def _getEventAttribs(event:HistoryEvent):String = {

      if(event.getWorkflowExecutionStartedEventAttributes != null) {
        return event.getWorkflowExecutionStartedEventAttributes.toString
      }
      if(event.getWorkflowExecutionCompletedEventAttributes != null) {
        return event.getWorkflowExecutionCompletedEventAttributes.toString
      }
      if(event.getCompleteWorkflowExecutionFailedEventAttributes != null) {
        return event.getCompleteWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getWorkflowExecutionFailedEventAttributes != null) {
        return event.getWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getFailWorkflowExecutionFailedEventAttributes != null) {
        return event.getFailWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getWorkflowExecutionTimedOutEventAttributes != null) {
        return event.getWorkflowExecutionTimedOutEventAttributes.toString
      }
      if(event.getWorkflowExecutionCanceledEventAttributes != null) {
        return event.getWorkflowExecutionCanceledEventAttributes.toString
      }
      if(event.getCancelWorkflowExecutionFailedEventAttributes != null) {
        return event.getCancelWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getWorkflowExecutionContinuedAsNewEventAttributes != null) {
        return event.getWorkflowExecutionContinuedAsNewEventAttributes.toString
      }
      if(event.getContinueAsNewWorkflowExecutionFailedEventAttributes != null) {
        return event.getContinueAsNewWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getWorkflowExecutionTerminatedEventAttributes != null) {
        return event.getWorkflowExecutionTerminatedEventAttributes.toString
      }
      if(event.getWorkflowExecutionCancelRequestedEventAttributes != null) {
        return event.getWorkflowExecutionCancelRequestedEventAttributes.toString
      }
      if(event.getDecisionTaskScheduledEventAttributes != null) {
        return event.getDecisionTaskScheduledEventAttributes.toString
      }
      if(event.getDecisionTaskStartedEventAttributes != null) {
        return event.getDecisionTaskStartedEventAttributes.toString
      }
      if(event.getDecisionTaskCompletedEventAttributes != null) {
        return event.getDecisionTaskCompletedEventAttributes.toString
      }
      if(event.getDecisionTaskTimedOutEventAttributes != null) {
        return event.getDecisionTaskTimedOutEventAttributes.toString
      }
      if(event.getActivityTaskScheduledEventAttributes != null) {
        return event.getActivityTaskScheduledEventAttributes.toString
      }
      if(event.getActivityTaskStartedEventAttributes != null) {
        return event.getActivityTaskStartedEventAttributes.toString
      }
      if(event.getActivityTaskCompletedEventAttributes != null) {
        return event.getActivityTaskCompletedEventAttributes.toString
      }
      if(event.getActivityTaskFailedEventAttributes != null) {
        return event.getActivityTaskFailedEventAttributes.toString
      }
      if(event.getActivityTaskTimedOutEventAttributes != null) {
        return event.getActivityTaskTimedOutEventAttributes.toString
      }
      if(event.getActivityTaskCanceledEventAttributes != null) {
        return event.getActivityTaskCanceledEventAttributes.toString
      }
      if(event.getActivityTaskCancelRequestedEventAttributes != null) {
        return event.getActivityTaskCancelRequestedEventAttributes.toString
      }
      if(event.getWorkflowExecutionSignaledEventAttributes != null) {
        return event.getWorkflowExecutionSignaledEventAttributes.toString
      }
      if(event.getMarkerRecordedEventAttributes != null) {
        return event.getMarkerRecordedEventAttributes.toString
      }
      if(event.getRecordMarkerFailedEventAttributes != null) {
        return event.getRecordMarkerFailedEventAttributes.toString
      }
      if(event.getTimerStartedEventAttributes != null) {
        return event.getTimerStartedEventAttributes.toString
      }
      if(event.getTimerFiredEventAttributes != null) {
        return event.getTimerFiredEventAttributes.toString
      }
      if(event.getTimerCanceledEventAttributes != null) {
        return event.getTimerCanceledEventAttributes.toString
      }
      if(event.getStartChildWorkflowExecutionInitiatedEventAttributes != null) {
        return event.getStartChildWorkflowExecutionInitiatedEventAttributes.toString
      }
      if(event.getChildWorkflowExecutionStartedEventAttributes != null) {
        return event.getChildWorkflowExecutionStartedEventAttributes.toString
      }
      if(event.getChildWorkflowExecutionCompletedEventAttributes != null) {
        return event.getChildWorkflowExecutionCompletedEventAttributes.toString
      }
      if(event.getChildWorkflowExecutionFailedEventAttributes != null) {
        return event.getChildWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getChildWorkflowExecutionTimedOutEventAttributes != null) {
        return event.getChildWorkflowExecutionTimedOutEventAttributes.toString
      }
      if(event.getChildWorkflowExecutionCanceledEventAttributes != null) {
        return event.getChildWorkflowExecutionCanceledEventAttributes.toString
      }
      if(event.getChildWorkflowExecutionTerminatedEventAttributes != null) {
        return event.getChildWorkflowExecutionTerminatedEventAttributes.toString
      }
      if(event.getSignalExternalWorkflowExecutionInitiatedEventAttributes != null) {
        return event.getSignalExternalWorkflowExecutionInitiatedEventAttributes.toString
      }
      if(event.getExternalWorkflowExecutionSignaledEventAttributes != null) {
        return event.getExternalWorkflowExecutionSignaledEventAttributes.toString
      }
      if(event.getSignalExternalWorkflowExecutionFailedEventAttributes != null) {
        return event.getSignalExternalWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getExternalWorkflowExecutionCancelRequestedEventAttributes != null) {
        return event.getExternalWorkflowExecutionCancelRequestedEventAttributes.toString
      }
      if(event.getRequestCancelExternalWorkflowExecutionInitiatedEventAttributes != null) {
        return event.getRequestCancelExternalWorkflowExecutionInitiatedEventAttributes.toString
      }
      if(event.getRequestCancelExternalWorkflowExecutionFailedEventAttributes != null) {
        return event.getRequestCancelExternalWorkflowExecutionFailedEventAttributes.toString
      }
      if(event.getScheduleActivityTaskFailedEventAttributes != null) {
        return event.getScheduleActivityTaskFailedEventAttributes.toString
      }
      if(event.getRequestCancelActivityTaskFailedEventAttributes != null) {
        return event.getRequestCancelActivityTaskFailedEventAttributes.toString
      }
      if(event.getStartTimerFailedEventAttributes != null) {
        return event.getStartTimerFailedEventAttributes.toString
      }
      if(event.getCancelTimerFailedEventAttributes != null) {
        return event.getCancelTimerFailedEventAttributes.toString
      }
      if(event.getStartChildWorkflowExecutionFailedEventAttributes != null) {
        return event.getStartChildWorkflowExecutionFailedEventAttributes.toString
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
    context.setResourceBase("src/main/webapp")
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
}
