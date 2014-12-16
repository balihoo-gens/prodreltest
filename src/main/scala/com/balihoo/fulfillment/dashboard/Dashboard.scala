package com.balihoo.fulfillment.dashboard

import java.io.File
import java.util

import com.balihoo.fulfillment.SWFHistoryConvertor
import com.balihoo.fulfillment.deciders._
import com.balihoo.fulfillment.workers.{FulfillmentWorkerTable, FulfillmentWorkerEntry}
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
      val fulfillment = new Fulfillment(SWFHistoryConvertor.jsonToSWFEvents(historyJson))
      new DecisionGenerator(fulfillment).makeDecisions()

      val sectionsJson = collection.mutable.Map[String, JsValue]()
//      fulfillment.categorized.categorize()
      for((name, section:FulfillmentSection) <- fulfillment.nameToSection) {
        section.evaluateParameters(fulfillment) // NOTE: This may have the undesirable side effect of changing section statuses (not actually in the workflow, but in the returned data)
        sectionsJson(name) = section.toJson
      }

      val jtimeline = Json.toJson(for(entry <- fulfillment.timeline.events) yield entry.toJson)

      Map(
        "timeline" -> jtimeline,
        "sections" -> Json.toJson(sectionsJson.toMap),
        "workflowId" -> Json.toJson(workflowId),
        "runId" -> Json.toJson(runId),
        "status" -> Json.toJson(fulfillment.status.toString),
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

    val workers = workerTable.get(dynamoAdapter.config.getString("domain"))

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

class AbstractCoordinatorServlet extends RestServlet {
  this: DynamoAdapterComponent
    with SploggerComponent =>

  val coordinatorTable = new FulfillmentCoordinatorTable with DynamoAdapterComponent with SploggerComponent {
    def dynamoAdapter = AbstractCoordinatorServlet.this.dynamoAdapter
    def splog = AbstractCoordinatorServlet.this.splog
  }

  get("/coordinator", (rsq:RestServletQuery) => {

    val coordinators = coordinatorTable.get()

    val coordinatorMap = collection.mutable.Map[String, JsValue]()
    for(coordinator:FulfillmentCoordinatorEntry <- coordinators) {
      coordinatorMap(coordinator.getInstance()) = coordinator.toJson
    }

    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(coordinatorMap.toMap)))
  })

}

class CoordinatorServlet(dyn: DynamoAdapter, splg: Splogger)
  extends AbstractCoordinatorServlet
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
    val coordinatorServlet = new CoordinatorServlet(new DynamoAdapter(cfg), splog)
    val workflowServlet = new WorkflowServlet(new SWFAdapter(cfg, splog, true), splog)

    context.addServlet(new ServletHolder(workerServlet), "/worker/*")
    context.addServlet(new ServletHolder(coordinatorServlet), "/coordinator/*")
    context.addServlet(new ServletHolder(workflowServlet), "/workflow/*")

    val server = new Server(cfg.getInt("port"))
    server.setHandler(context)
    server.start()
    checkPing
    server.join()
    splog.info(s"Terminated $name")
  }

  def checkPing = {
    var done = false
    val getch = new Getch
    getch.addMapping(Seq("quit", "Quit", "exit", "Exit"), () => { println("\nExiting...\n");done = true})
    getch.addMapping(Seq("ping"), () => { println("pong") } )

    getch.doWith {
      while(!done) {
        Thread.sleep(100)
      }
    }
  }

  def isRunningFromJar:Boolean = {
    new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath).isFile
  }
}
