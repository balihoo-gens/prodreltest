package com.balihoo.fulfillment.dashboard

import java.text.SimpleDateFormat
import java.util.Date

import com.balihoo.fulfillment.deciders.{FulfillmentSection, SectionMap}
import play.api.libs.json._

import scala.collection.JavaConverters._

import javax.servlet.http.HttpServletResponse

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.{DynamoAdapter, SWFAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext

class ExecutionUtility(swfAdapter: SWFAdapter) {

  val DAY_IN_MS = 1000 * 60 * 60 * 24
  val dateFormatter = new SimpleDateFormat()

  def executionHistory():List[JsValue] = {

    val oldest = new Date(System.currentTimeMillis() - (7 * DAY_IN_MS))
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
}



class WorkflowServlet(swfAdapter: SWFAdapter) extends DashServlet {

  val domain = swfAdapter.config.getString("domain")
  val eu = new ExecutionUtility(swfAdapter)

  get("/workflow/history", (dtrans:DashTransaction) => {
    dtrans.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(eu.executionHistory())))
  })

  get("/workflow/sections", (dtrans:DashTransaction) => {
    dtrans.respondJson(HttpServletResponse.SC_OK
      ,Json.stringify(Json.toJson(eu.workflowSections(
        dtrans.getRequiredParameter("runId")
        ,dtrans.getRequiredParameter("workflowId")
      ))))
  })

}

class WorkerServlet(dynamoAdapter: DynamoAdapter) extends DashServlet {

//  get("/worker", (dtrans:DashTransaction) => {
//    response.setContentType("application/json")
//    response.setStatus(HttpServletResponse.SC_OK)
//    response.getWriter.println("""{ "message" : "HELLO THERE" }""")
//  })

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