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

class WorkflowInspector {
  this: SWFAdapterComponent =>

  def executionHistory():List[JsValue] = {

    val oldest = new Date(System.currentTimeMillis() - (70 * UTCFormatter.DAY_IN_MS))
    val latest = new Date(System.currentTimeMillis())
    val filter = new ExecutionTimeFilter
    filter.setOldestDate(oldest)
    filter.setLatestDate(latest)

    val lreq = new ListClosedWorkflowExecutionsRequest()
    lreq.setDomain(swfAdapter.config.getString("domain"))
//    lreq.setReverseOrder(true)
    lreq.setStartTimeFilter(filter)
    val infos = swfAdapter.client.listClosedWorkflowExecutions(lreq)

    val m = new collection.mutable.MutableList[JsValue]()
    for(info:WorkflowExecutionInfo <- infos.getExecutionInfos.asScala) {
      m += Json.toJson(Map(
        "workflowId" -> Json.toJson(info.getExecution.getWorkflowId),
        "runId" -> Json.toJson(info.getExecution.getRunId),
        "closeStatus" -> Json.toJson(info.getCloseStatus),
        "closeTimestamp" -> Json.toJson(info.getCloseTimestamp.toString),
        "startTimestamp" -> Json.toJson(info.getStartTimestamp.toString),
        "tagList" -> Json.toJson(info.getTagList.asScala)
       ))
    }

    m.toList
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
    for((name, section:FulfillmentSection) <- sectionMap.map) {
      sections(name) = Json.toJson(Map(
        "status" -> Json.toJson(section.status.toString),
        "notes" -> Json.toJson(section.notes),
        "value" -> Json.toJson(section.value),
        "input" -> Json.toJson(section.jsonNode)
      ))

    }

    top("notes") = Json.toJson(sectionMap.notes.toList)
    top("sections") = Json.toJson(sections.toMap)
    top("workflowId") = Json.toJson(workflowId)
    top("runId") = Json.toJson(runId)

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

  get("/workflow/history", (rsq:RestServletQuery) => {
    rsq.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(wi.executionHistory())))
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
