package com.balihoo.fulfillment.dashboard

import java.io.File
import java.util

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
import com.balihoo.fulfillment.util._

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
      val fulfillmentSections = new FulfillmentSections(new util.ArrayList[HistoryEvent]())
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
      val sections = new FulfillmentSections(events)
      new DecisionGenerator(sections).makeDecisions(false)

      val sectionsJson = collection.mutable.Map[String, JsValue]()
      for((name, section:FulfillmentSection) <- sections.nameToSection) {
        sectionsJson(name) = section.toJson
      }

      val jtimeline = Json.toJson(for(entry <- sections.timeline.events) yield entry.toJson)
      val executionHistory = Json.toJson(for(event:HistoryEvent <- collectionAsScalaIterable(events)) yield Json.obj(
        "type" -> event.getEventType,
        "id" -> event.getEventId.toString,
        "timestamp" -> UTCFormatter.format(event.getEventTimestamp),
        "attributes" -> _getEventAttribs(event)
      ))

      Map(
        "timeline" -> jtimeline,
        "sections" -> Json.toJson(sectionsJson.toMap),
        "workflowId" -> Json.toJson(workflowId),
        "runId" -> Json.toJson(runId),
        "input" -> Json.toJson(events.get(0).getWorkflowExecutionStartedEventAttributes.getInput),
        "resolution" -> Json.toJson(sections.resolution),
        "history" -> executionHistory
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
    with WorkflowUpdaterComponent
    with SploggerComponent =>

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

class WorkflowServlet(swf: SWFAdapter, splg: Splogger)
  extends AbstractWorkflowServlet
    with WorkflowInspectorComponent
    with WorkflowInitiatorComponent
    with WorkflowUpdaterComponent
    with SWFAdapterComponent
    with SploggerComponent {
  private val _inspector = new WorkflowInspector(swf)
  private val _initiator = new WorkflowInitiator(swf)
  private val _updater = new WorkflowUpdater(swf)
  def swfAdapter = swf
  def workflowInspector = _inspector
  def workflowInitiator = _initiator
  def workflowUpdater = _updater
  def splog = splg
}

class AbstractWorkerServlet extends RestServlet {
  this: DynamoAdapterComponent
    with SploggerComponent =>

  val workerTable = new FulfillmentWorkerTable with DynamoAdapterComponent with SploggerComponent {
    def dynamoAdapter = AbstractWorkerServlet.this.dynamoAdapter
    def splog = AbstractWorkerServlet.this.splog
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

class WorkerServlet(dyn: DynamoAdapter, splg: Splogger)
  extends AbstractWorkerServlet
    with DynamoAdapterComponent
    with SploggerComponent {
  def dynamoAdapter = dyn
  def splog = splg
}

object dashboard {
  def main(args: Array[String]) {

    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    splog.info(s"Started $name")
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))

    val context = new WebAppContext()
    context setContextPath "/"
    if(isRunningFromJar) {
      context.setWar(context.getClass.getClassLoader.getResource("webapp").toExternalForm)
    } else {
      context.setResourceBase("src/main/webapp")
    }
    context.setWelcomeFiles(Array[String]("index.html"))

    val workerServlet = new WorkerServlet(new DynamoAdapter(cfg), splog)
    val workflowServlet = new WorkflowServlet(new SWFAdapter(cfg), splog)

    context.addServlet(new ServletHolder(workerServlet), "/worker/*")
    context.addServlet(new ServletHolder(workflowServlet), "/workflow/*")

    val server = new Server(cfg.getInt("port"))
    server.setHandler(context)
    server.start()
    server.join()
    splog.info(s"Terminated $name")
  }

  def isRunningFromJar:Boolean = {
    new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath).isFile
  }
}
