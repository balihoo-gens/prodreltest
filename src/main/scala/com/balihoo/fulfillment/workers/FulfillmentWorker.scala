package com.balihoo.fulfillment.workers

//scala imports

import scala.collection.mutable
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.util.{Success, Failure}
import scala.concurrent.{Future, Await, Promise, ExecutionContext}
import scala.concurrent.duration._

//java imports
import java.util.UUID.randomUUID
import java.util.concurrent.{TimeUnit, Executors}

//aws imports
import com.amazonaws.services.simpleworkflow.model._

//play imports
import play.api.libs.json._

//other external
import org.joda.time.DateTime
import org.keyczar.Crypter

//local imports
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._

abstract class FulfillmentWorker extends WithResources {
  this: LoggingWorkflowAdapter =>

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  val instanceId = randomUUID().toString

  def domain = swfAdapter.domain
  def name = swfAdapter.name
  def version = swfAdapter.version
  def taskListName = swfAdapter.taskListName
  def taskList = swfAdapter.taskList

  val crypter = new Crypter("config/crypto")

  val defaultTaskHeartbeatTimeout = swfAdapter.config.getString("default_task_heartbeat_timeout")
  val defaultTaskScheduleToCloseTimeout = swfAdapter.config.getString("default_task_schedule_to_close_timeout")
  val defaultTaskScheduleToStartTimeout = swfAdapter.config.getString("default_task_schedule_to_start_timeout")
  val defaultTaskStartToCloseTimeout = swfAdapter.config.getString("default_task_start_to_close_timeout")

  val workerTable = new FulfillmentWorkerTable with DynamoAdapterComponent with SploggerComponent {
    def dynamoAdapter = FulfillmentWorker.this.dynamoAdapter
    def splog = FulfillmentWorker.this.splog
  }

  val taskResolutions = new mutable.Queue[TaskResolution]()

  val entry = new FulfillmentWorkerEntry
  entry.tableName = workerTable.dynamoAdapter.config.getString("worker_status_table")
  entry.setInstance(instanceId)
  entry.setHostAddress(HostIdentity.getHostAddress)
  entry.setActivityName(name)
  entry.setActivityVersion(version)
  entry.setSpecification(getSpecification.toString)
  entry.setDomain(domain)
  entry.setStatus("--")
  entry.setResolutionHistory("[]")
  entry.setStart(UTCFormatter.format(DateTime.now))

  var completedTasks:Int = 0
  var failedTasks:Int = 0
  var canceledTasks:Int = 0

  var updateCounter:Int = 0

  declareWorker()

  var _lastTaskToken: String = ""
  var _doneFuture: Option[Future[Boolean]] = None

  def work() = {

    registerActivityType()

    updateStatus("Starting")

    val getch = new Getch
    _doneFuture = Some(setupQuitKey(getch))
    getch.doWith {
      handleTaskFuture(swfAdapter.getTask)
      Await.result(_doneFuture.get, Duration.Inf )
    }
    shutdown()
  }

  def setupQuitKey(getch: Getch): Future[Boolean] = {
    val donePromise = Promise[Boolean]()
    getch.addMapping(
      Seq("quit", "Quit", "exit", "Exit"), () => {
        //stdin received quit request. Respond on stdout
        println("Quitting...")
        updateStatus("Terminated by user", "WARN")
        donePromise.success(true)
      }
    )
    //respond to ping
    getch.addMapping(Seq("ping"), () => { println("pong") } )

    //return the future that will succeed when termination is requested
    donePromise.future
  }

  def shutdown() = {
    updateStatus("Exiting")
    val excsvc = swfAdapter.client.getExecutorService
    excsvc.shutdown()
    if (!excsvc.awaitTermination(3, TimeUnit.SECONDS)) {
      val tasks = excsvc.shutdownNow()
      splog.warning("Performing hard shutdown with remaining tasks: " + tasks.length)
      Thread.sleep(100)
      swfAdapter.client.shutdown()
    }
  }

  def handleTaskFuture(taskFuture: Future[Option[ActivityTask]]):Unit = {
    updateStatus("Polling")
    taskFuture onComplete {
      case Success(taskopt) =>
        taskopt match {
          case Some(task) =>
            try {
              _lastTaskToken = task.getTaskToken
              val shortToken = _lastTaskToken takeRight 10
              updateStatus("Processing task.." + shortToken )
              val start = System.currentTimeMillis()
              handleTask(getSpecification.getParameters(task.getInput))
              val time = System.currentTimeMillis() - start
              splog.info(s"Task processed time=$time")
            } catch {
              case e:Exception =>
                e.printStackTrace()
                splog.warning(s"activity failed: exception=${e.toString}")
                failTask(s"""{"$name": "${e.toString}"}""", e.getMessage)
            } finally {
              closeResources()
            }
          case None =>
            splog.info("no task available")
        }

        if (_doneFuture.nonEmpty || _doneFuture.get.isCompleted) {
          //get the next one
          handleTaskFuture(swfAdapter.getTask)
        } else {
          updateStatus("Termination requested; not getting a new task", "WARNING")
        }

      //this is unusual; when no task is available, success is returned above with None
      case Failure(e) =>
        val ms = 1000
        updateStatus(s"Polling Exception ${e.getMessage}: waiting ${ms}ms before retrying", "EXCEPTION")
        if (_doneFuture.nonEmpty || _doneFuture.get.isCompleted) {
          Thread.sleep(ms)
          //get the next one
          handleTaskFuture(swfAdapter.getTask)
        } else {
          updateStatus("Termination requested; not getting a new task", "WARNING")
        }
    }
  }

  def getSpecification: ActivitySpecification

  def handleTask(params:ActivityParameters)

  def withTaskHandling(code: => String) {
    try {
      val result:String = code
      completeTask(result)
    } catch {
      case cancel:CancelTaskException =>
        splog("INFO", cancel.details + " " + cancel.getMessage)
        cancelTask(cancel.details)
      case e:Exception =>
        failTask("Task Failed", e.getMessage + " " + e.getStackTraceString take 150)
    }
  }

  def declareWorker() = {
    val status = s"Declaring $name $domain $taskListName"
    splog("INFO",status)
    entry.setLast(UTCFormatter.format(DateTime.now))
    entry.setStatus(status)
    workerTable.insert(entry)
  }

  def updateStatus(status:String, level:String="INFO") = {
    try {
      updateCounter += 1
      entry.setLast(UTCFormatter.format(DateTime.now))
      entry.setStatus(status)
      workerTable.update(entry)
      splog(level,status)
    } catch {
      case e:Exception =>
        //splog will print to stdout on any throwable, or log to the default logfile
        splog("ERROR", s"$name failed to update status: ${e.toString}")
    }
  }

  private def _resolveTask(resolution:TaskResolution) = {
    if(taskResolutions.size == 20) {
      taskResolutions.dequeue()
    }
    taskResolutions.enqueue(resolution)
    entry.setResolutionHistory(Json.stringify(
      Json.toJson(for(res <- taskResolutions) yield res.toJson)
    ))
    updateStatus(resolution.resolution)
  }

  def completeTask(result:String) = {
    completedTasks += 1
    try {
      if (_lastTaskToken != null && _lastTaskToken.nonEmpty) {
        val response:RespondActivityTaskCompletedRequest = new RespondActivityTaskCompletedRequest
        response.setTaskToken(_lastTaskToken)
        response.setResult(getSpecification.result match {
          case e:EncryptedActivityResult => crypter.encrypt(result)
          case _ => result
        })
        swfAdapter.client.respondActivityTaskCompleted(response)
      } else {
        throw new Exception("empty task token")
      }
    } catch {
      case e:Exception =>
        splog.error(s"error completing task: ${e.getMessage}")
    }
    _resolveTask(new TaskResolution("Completed", result))
  }

  def failTask(reason:String, details:String) = {
    failedTasks += 1
    try {
      if (_lastTaskToken != null && _lastTaskToken.nonEmpty) {
        val response:RespondActivityTaskFailedRequest = new RespondActivityTaskFailedRequest
        response.setTaskToken(_lastTaskToken)
        // http://docs.aws.amazon.com/amazonswf/latest/apireference/API_ActivityTaskFailedEventAttributes.html
        response.setReason(reason take 256)
        response.setDetails(details take 32768)
        swfAdapter.client.respondActivityTaskFailed(response)
      } else {
        throw new Exception("empty task token")
      }
    } catch {
      case e:Exception =>
        splog.error(s"error failing task: ${e.getMessage}")
    }
    _resolveTask(new TaskResolution("Failed", s"$reason $details"))
  }

  def cancelTask(details:String) = {
    canceledTasks += 1
    val response:RespondActivityTaskCanceledRequest = new RespondActivityTaskCanceledRequest
    response.setTaskToken(_lastTaskToken)
    // http://docs.aws.amazon.com/amazonswf/latest/apireference/API_ActivityTaskCanceledEventAttributes.html
    response.setDetails(details take 32768)
    swfAdapter.client.respondActivityTaskCanceled(response)
    _resolveTask(new TaskResolution("Canceled", details))
  }

  def registerActivityType() = {
    val activityType = new ActivityType()
      .withName(name)
      .withVersion(version)

    val describe = new DescribeActivityTypeRequest()
      .withActivityType(activityType)
      .withDomain(domain)

    try {
      swfAdapter.client.describeActivityType(describe)
    } catch {
      case ure: UnknownResourceException =>
        updateStatus(s"Registering new Activity ($name,$version)")
        val registrationRequest = new RegisterActivityTypeRequest()
          .withDomain(domain)
          .withName(name)
          .withVersion(version)
          .withDefaultTaskList(taskList)
          .withDefaultTaskHeartbeatTimeout(defaultTaskHeartbeatTimeout)
          .withDefaultTaskScheduleToCloseTimeout(defaultTaskScheduleToCloseTimeout)
          .withDefaultTaskScheduleToStartTimeout(defaultTaskScheduleToStartTimeout)
          .withDefaultTaskStartToCloseTimeout(defaultTaskStartToCloseTimeout)
        swfAdapter.client.registerActivityType(registrationRequest)
      case e: Exception =>
        updateStatus(s"Failed to register Activity ($name,$version)")
    }
  }

}

class CancelTaskException(val exception:String, val details:String) extends Exception(exception)

class TaskResolution(val resolution:String, val details:String) {
  val when = UTCFormatter.format(DateTime.now)
  def toJson:JsValue = {
    Json.toJson(Map(
      "resolution" -> Json.toJson(resolution),
      "when" -> Json.toJson(when),
      "details" -> Json.toJson(details)
    ))
  }
}


abstract class FulfillmentWorkerApp {
  def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker

  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    splog("INFO", s"Started $name")
    try {
      val cfg = PropertiesLoader(args, name)
      val worker = createWorker(cfg, splog)
      worker.work()
    }
    catch {
      case e:Exception =>
        splog.exception(e.getMessage)
    }
    splog.info(s"Terminated $name")
  }
}

