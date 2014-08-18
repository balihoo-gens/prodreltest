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
      val name = new ValidSWFName(swfAdapter.config.getString("name"))
      val version = new ValidSWFVersion(swfAdapter.config.getString("version"))
      val taskListName = new ValidSWFName(name + version)
      executionRequest.setWorkflowType(new WorkflowType().withName(name).withVersion(version))
      executionRequest.setTaskList(new TaskList().withName(taskListName))
      swfAdapter.client.startWorkflowExecution(executionRequest).getRunId
    }
  }

  class WorkflowInitiator(swf: SWFAdapter) extends AbstractWorkflowInitiator with SWFAdapterComponent {
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

      val jtimeline = Json.toJson(for(entry <- sectionMap.timeline.events) yield entry.toJson)

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

  class WorkflowInspector(swf: SWFAdapter) extends AbstractWorkflowInspector with SWFAdapterComponent {
    def swfAdapter = swf
  }
}


class AbstractWorkflowServlet extends RestServlet {
  this: SWFAdapterComponent
    with WorkflowInspectorComponent
    with WorkflowInitiatorComponent =>

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
    with SWFAdapterComponent {
  private val _inspector = new WorkflowInspector(swf)
  private val _initiator = new WorkflowInitiator(swf)
  def swfAdapter = swf
  def workflowInspector = _inspector
  def workflowInitiator = _initiator
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
