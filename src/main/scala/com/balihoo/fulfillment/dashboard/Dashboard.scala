package com.balihoo.fulfillment.dashboard

import java.util.Date

import com.balihoo.fulfillment.deciders.{DecisionGenerator, CategorizedSections, FulfillmentSection, SectionMap}
import com.balihoo.fulfillment.workers.{UTCFormatter, FulfillmentWorkerTable, FulfillmentWorkerEntry}
import play.api.libs.json._

import scala.collection.JavaConverters._

import javax.servlet.http.HttpServletResponse

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.adapters.{DynamoAdapter, SWFAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext

class WorkflowInitiator(swfAdapter: SWFAdapter) {

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

class WorkflowInspector(swfAdapter: SWFAdapter) {

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

//    val oldest = new Date(System.currentTimeMillis() - (70 * UTCFormatter.DAY_IN_MS))
//    val latest = new Date(System.currentTimeMillis() + (10 * UTCFormatter.DAY_IN_MS))
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

    val jtimeline = Json.toJson(for(entry <- sectionMap.timeline) yield entry.toJson)

    top("timeline") = jtimeline
    top("sections") = Json.toJson(sections.toMap)
    top("workflowId") = Json.toJson(workflowId)
    top("runId") = Json.toJson(runId)
    top("input") = Json.toJson(history.getEvents.get(0).getWorkflowExecutionStartedEventAttributes.getInput)

    top.toMap
  }

  def environment() = {
    Json.toJson(Map(
      "domain" -> Json.toJson(swfAdapter.domain),
      "region" -> Json.toJson(swfAdapter.region.getName)
    ))
  }
}



class WorkflowServlet(swfAdapter: SWFAdapter) extends RestServlet {

  val wi = new WorkflowInspector(swfAdapter)
  val wfi = new WorkflowInitiator(swfAdapter)

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

class WorkerServlet(dynamoAdapter:DynamoAdapter) extends RestServlet {

  val workerTable = new FulfillmentWorkerTable(dynamoAdapter)

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
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))

    val swfAdapter = new SWFAdapter(config)
    val dynamoAdapter = new DynamoAdapter(config)

    val server = new Server(config.getInt("port"))
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    context.setWelcomeFiles(Array[String]("index.html"))

    context.addServlet(new ServletHolder(new WorkerServlet(dynamoAdapter)), "/worker/*")
    context.addServlet(new ServletHolder(new WorkflowServlet(swfAdapter)), "/workflow/*")

    server.setHandler(context)
    server.start()
    server.join()
  }
}
