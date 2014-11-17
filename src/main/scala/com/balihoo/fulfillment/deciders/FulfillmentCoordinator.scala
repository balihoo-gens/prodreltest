package com.balihoo.fulfillment.deciders

import java.util.UUID.randomUUID

import com.balihoo.fulfillment.SWFHistoryConvertor
import org.joda.time.DateTime

import scala.language.implicitConversions
import scala.collection.JavaConversions._

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.util._

object Constants {
  final val delimiter = "##"
}

abstract class AbstractFulfillmentCoordinator {
  this: SploggerComponent
  with SWFAdapterComponent
  with DynamoAdapterComponent =>

  val instanceId = randomUUID().toString

  //can't have constructor code using the self type reference
  // unless it was declared 'lazy'. If not, swfAdapter is still null
  // and will throw a NullPointerException at this time.
  val domain = new SWFName(swfAdapter.config.getString("domain"))
  val workflowName = new SWFName(swfAdapter.config.getString("workflowName"))
  val workflowVersion = new SWFVersion(swfAdapter.config.getString("workflowVersion"))
  val taskListName = new SWFName(workflowName + workflowVersion)

  val taskList: TaskList = new TaskList()
    .withName(taskListName)

  val taskReq: PollForDecisionTaskRequest = new PollForDecisionTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  val coordinatorTable = new FulfillmentCoordinatorTable with DynamoAdapterComponent with SploggerComponent {
    def dynamoAdapter = AbstractFulfillmentCoordinator.this.dynamoAdapter
    def splog = AbstractFulfillmentCoordinator.this.splog
  }

// TODO Implement me!
//  val taskResolutions = new mutable.Queue[TaskResolution]()

  val entry = new FulfillmentCoordinatorEntry
  entry.tableName = coordinatorTable.dynamoAdapter.config.getString("coordinator_status_table")
  entry.setInstance(instanceId)
  entry.setHostAddress(HostIdentity.getHostAddress)
  entry.setWorkflowName(workflowName)
  entry.setWorkflowVersion(workflowVersion)
  entry.setSpecification("[]")
  entry.setDomain(domain)
  entry.setStatus("--")
  entry.setResolutionHistory("[]")
  entry.setStart(UTCFormatter.format(DateTime.now))

  def coordinate() = {

    splog.info(s"$domain $taskListName")

    declareCoordinator()

    var done = false
    val getch = new Getch
    getch.addMapping(Seq("quit", "Quit", "exit", "Exit"), () => {splog.info("\nExiting...\n");done = true})
    getch.addMapping(Seq("ping"), () => { println("pong") } )

    getch.doWith {
      while(!done) {
        try {
          updateStatus("Polling")
          val task: DecisionTask = swfAdapter.client.pollForDecisionTask(taskReq)

          if(task.getTaskToken != null) {

            updateStatus("Processing "+ task.getTaskToken takeRight 12)
            splog.info(s"processing token ${task.getTaskToken}")
            val sections = new Fulfillment(SWFHistoryConvertor.historyToSWFEvents(task.getEvents))
            val decisions = new DecisionGenerator(sections).makeDecisions()

            val response: RespondDecisionTaskCompletedRequest = new RespondDecisionTaskCompletedRequest
            response.setTaskToken(task.getTaskToken)
            response.setDecisions(asJavaCollection(decisions))
            swfAdapter.client.respondDecisionTaskCompleted(response)
          }
        } catch {
          case se: java.net.SocketException =>
          // these happen.. no biggie.
          case e: Exception =>
            splog.error(e.getMessage)
        }
      }
    }
    splog.info("Done. Cleaning up...")
  }

  def declareCoordinator() = {
    val status = s"Declaring $domain $taskListName"
    splog("INFO",status)
    entry.setLast(UTCFormatter.format(DateTime.now))
    entry.setStatus(status)
    coordinatorTable.insert(entry)
  }

  def updateStatus(status:String, level:String="INFO") = {
    try {
      entry.setLast(UTCFormatter.format(DateTime.now))
      entry.setStatus(status)
      coordinatorTable.update(entry)
      splog(level,status)
    } catch {
      case e:Exception =>
        //splog will print to stdout on any throwable, or log to the default logfile
        splog("ERROR", s"Failed to update status: ${e.toString}")
    }
  }
}


class FulfillmentCoordinator(swf: SWFAdapter, dyn:DynamoAdapter, splogger: Splogger)
  extends AbstractFulfillmentCoordinator
  with SploggerComponent
  with SWFAdapterComponent
  with DynamoAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def splog = splogger
}

object coordinator {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    splog.info(s"Started $name")
    try {
      val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
      splog.debug("Created PropertiesLoader")
      val swf = new SWFAdapter(config, splog, true)
      splog.debug("Created SWFAdapter")
      val dyn = new DynamoAdapter(config)
      splog.debug("Created DynamoAdapter")
      val fc = new FulfillmentCoordinator(swf, dyn, splog)
      splog.debug("Created FulfillmentCoordinator")
      fc.coordinate()
    }
    catch {
      case e:Exception =>
        splog.error(e.getMessage)
    }
    splog("INFO", s"Terminated $name")
  }
}
