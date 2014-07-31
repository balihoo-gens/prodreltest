package com.balihoo.fulfillment.dashboard

import java.util.Date

import com.balihoo.fulfillment.deciders.{DecisionGenerator, CategorizedSections, FulfillmentSection, SectionMap}
import com.balihoo.fulfillment.workers.{UTCFormatter, FulfillmentWorkerTable, FulfillmentWorkerEntry}
import play.api.libs.json._

import scala.collection.JavaConverters._

import javax.servlet.http.HttpServletResponse

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext

class WorkflowInitiator {
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

class WorkflowUpdater {
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

class WorkflowInspector {
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

    val history = swfAdapter.client.getWorkflowExecutionHistory(req)
    val sectionMap = new SectionMap(history.getEvents)
    val categorized = new CategorizedSections(sectionMap)
    new DecisionGenerator(categorized, sectionMap).makeDecisions()

    val sections = collection.mutable.Map[String, JsValue]()
    for((name, section:FulfillmentSection) <- sectionMap.nameToSection) {
      sections(name) = section.toJson
    }

    val jtimeline = Json.toJson(for(entry <- sectionMap.timeline.events) yield entry.toJson)

    top("timeline") = jtimeline
    top("sections") = Json.toJson(sections.toMap)
    top("workflowId") = Json.toJson(workflowId)
    top("runId") = Json.toJson(runId)
    top("input") = Json.toJson(history.getEvents.get(0).getWorkflowExecutionStartedEventAttributes.getInput)
    top("resolution") = Json.toJson(sectionMap.resolution)

    top.toMap
  }

  def environment() = {
    Json.toJson(Map(
      "domain" -> Json.toJson(swfAdapter.domain),
      "region" -> Json.toJson(swfAdapter.region.getName)
    ))
  }
}



class WorkflowServlet extends RestServlet {
  this: SWFAdapterComponent =>

  val wi = new WorkflowInspector with SWFAdapterComponent {
    def swfAdapter = WorkflowServlet.this.swfAdapter
  }
  val wfi = new WorkflowInitiator with SWFAdapterComponent {
    def swfAdapter = WorkflowServlet.this.swfAdapter
  }
  val wfu = new WorkflowUpdater with SWFAdapterComponent {
    def swfAdapter = WorkflowServlet.this.swfAdapter
  }

  get("/workflow/history", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(wi.executionHistory(
          UTCFormatter.dateFormat.parse(rsq.getRequiredParameter("startDate")),
          UTCFormatter.dateFormat.parse(rsq.getRequiredParameter("endDate"))
      ))))
  })

  get("/workflow/detail", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(wi.workflowSections(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
      ))))
  })

  get("/workflow/update", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(wfu.update(
        rsq.getRequiredParameter("runId")
        ,rsq.getRequiredParameter("workflowId")
        ,rsq.getRequiredParameter("input")
      ))))
  })

  get("/workflow/environment", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(wi.environment())))
  })

  get("/workflow/initiate", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(wfi.initate(
        rsq.getRequiredParameter("id"),
        rsq.getRequiredParameter("input"),
        rsq.getOptionalParameter("tags", "").split(",").toList
      ))))
  })

}

class WorkerServlet extends RestServlet {
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

object dashboard {
  def main(args: Array[String]) {

    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))

    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    context.setWelcomeFiles(Array[String]("index.html"))

    val workerServlet = new WorkerServlet with DynamoAdapterComponent {
      def dynamoAdapter = DynamoAdapter(cfg)
    }

    val workflowServlet = new WorkflowServlet with SWFAdapterComponent {
      def swfAdapter = SWFAdapter(cfg)
    }

    context.addServlet(new ServletHolder(workerServlet), "/worker/*")
    context.addServlet(new ServletHolder(workflowServlet), "/workflow/*")

    val server = new Server(cfg.getInt("port"))
    server.setHandler(context)
    server.start()
    server.join()
  }
}
