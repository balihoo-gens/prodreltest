package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model._
import com.amazonaws.handlers.AsyncHandler

import scala.concurrent.{Future, Promise, ExecutionContext}
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._

import java.util.concurrent.Executors

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: AbstractSWFAdapter with PropertiesLoaderComponent
}

abstract class AbstractSWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient]  {
  this: PropertiesLoaderComponent
    with SploggerComponent =>

  private lazy val _name = new SWFName(config.getString("name"))
  private lazy val _domain = config.getString("domain")
  private lazy val _version = new SWFVersion(config.getString("version"))
  private lazy val _taskListName = new SWFName(_name + _version)
  private lazy val _taskList: TaskList = new TaskList().withName(_taskListName)
  private lazy val _workflowName = new SWFName(config.getString("workflowName"))
  private lazy val _workflowVersion = new SWFVersion(config.getString("workflowVersion"))
  private lazy val _workflowTaskListName = new SWFName(workflowName+workflowVersion)
  private lazy val _workflowExecutionStartToCloseTimeout = config.getOrElse("workflowExecutionStartToCloseTimeout", "3000000")
  private lazy val _workflowTaskStartToCloseTimeout = config.getOrElse("workflowTaskStartToCloseTimeout", "3000000")
  private lazy val _workflowChildPolicy = config.getOrElse("workflowChildPolicy", "TERMINATE")

  //longpoll by default, unless config says "longpoll=false"
  protected val _longPoll = config.getOrElse("longpoll", default=true)
  //use 10 threads in the threadpool by default, unless the config says otherwise
  protected val _threadcount = config.getOrElse("threadcount", default=10)

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(_threadcount))

  def name = _name
  def domain = _domain
  def version = _version
  def taskListName = _taskListName
  def taskList = _taskList
  def workflowName = _workflowName
  def workflowVersion = _workflowVersion
  def workflowTaskListName = _workflowTaskListName
  def workflowExecutionStartToCloseTimeout = _workflowExecutionStartToCloseTimeout
  def workflowTaskStartToCloseTimeout = _workflowTaskStartToCloseTimeout
  def workflowChildPolicy = _workflowChildPolicy

  protected val taskReq: PollForActivityTaskRequest = new PollForActivityTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)
  protected val countReq: CountPendingActivityTasksRequest = new CountPendingActivityTasksRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  def getTask: Future[Option[ActivityTask]]  = {
    val taskPromise = Promise[Option[ActivityTask]]()

    object activityPollHandler extends AsyncHandler[PollForActivityTaskRequest, ActivityTask] {
      override def onSuccess(req:PollForActivityTaskRequest, task:ActivityTask) {
        splog.debug("poll returned")
        if (task != null && task.getTaskToken != null) {
          taskPromise.success(Some(task))
        } else {
          taskPromise.success(None)
        }
      }
      override def onError(e:Exception) {
        splog.debug("poll failed")
        taskPromise.failure(e)
      }
    }

    object activityCountHandler extends AsyncHandler[CountPendingActivityTasksRequest, PendingTaskCount] {
      override def onSuccess(req:CountPendingActivityTasksRequest, count:PendingTaskCount) {
        splog.debug("count poll returned")
        if (count.getCount > 0) {
          client.pollForActivityTaskAsync(taskReq, activityPollHandler)
        } else {
          Thread.sleep(1000)
          taskPromise.success(None)
        }
      }
      override def onError(e:Exception) {
        splog.debug("count poll failed")
        taskPromise.failure(e)
      }
    }

    if (_longPoll) {
      splog.debug("using longpoll")
      client.pollForActivityTaskAsync(taskReq, activityPollHandler)
    } else {
      splog.debug("checking queue count")
      client.countPendingActivityTasksAsync(countReq, activityCountHandler)
    }

    taskPromise.future
  }

  def verifyDomain(autoRegister:Boolean = false) = {
    val ddr = new DescribeDomainRequest
    ddr.setName(domain)

    try {
      splog.info(s"Checking for domain '$domain'..")
      client.describeDomain(ddr)
      splog.info("Found it!")
    } catch {
      case ure:UnknownResourceException =>
        splog.warning(s"The domain '$domain' doesn't exist!")
        if(autoRegister) { registerDomain() }
    }
  }

  def registerDomain() = {
    val rdr = new RegisterDomainRequest
    rdr.setName(domain)
    rdr.setWorkflowExecutionRetentionPeriodInDays(config.getString("workflowRetentionDays"))
    rdr.setDescription(config.getString("domainDescription"))

    try {
      splog.info(s"Trying to register domain '$domain'")
      client.registerDomain(rdr)
    } catch {
      case daee:DomainAlreadyExistsException =>
        // Someone beat us to it! No worries..
      case lee:LimitExceededException =>
        // We've registered too many domains!?
        throw new Exception("Can't register new domain!", lee)
      //      case onpe:OperationNotPermittedException => FATAL DON'T CATCH
      //      case ace:AmazonClientException => FATAL DON'T CATCH
      //      case ase:AmazonServiceException => FATAL DON'T CATCH
      //      case e:Exception => FATAL DON'T CATCH
    }
  }

  def verifyWorkflowType(autoRegister:Boolean = false) = {
    val wt = new WorkflowType()
      .withName(workflowName)
      .withVersion(workflowVersion)
    val wtr = new DescribeWorkflowTypeRequest()
      .withDomain(domain)
      .withWorkflowType(wt)

    try {
      splog.info(s"Checking for workflow type '$workflowName:$workflowVersion'..")
      client.describeWorkflowType(wtr)
      splog.info("Found it!")
    } catch {
      case ure:UnknownResourceException =>
        splog.warning(s"The workflow type '${wt.getName}:${wt.getVersion}' doesn't exist!")
        if(autoRegister) { registerWorkflowType() }
    }
  }

  def registerWorkflowType() = {
    val taskList = new TaskList()
      .withName(workflowTaskListName)
    val rwtr = new RegisterWorkflowTypeRequest()
      .withDomain(domain)
      .withName(workflowName)
      .withVersion(workflowVersion)
      .withDefaultTaskList(taskList)
      .withDefaultChildPolicy(workflowChildPolicy)
      .withDefaultExecutionStartToCloseTimeout(workflowExecutionStartToCloseTimeout)
      .withDefaultTaskStartToCloseTimeout(workflowTaskStartToCloseTimeout)

    try {
      splog.info(s"Trying to register workflow type '$workflowName:$workflowVersion'")
      client.registerWorkflowType(rwtr)
    } catch {
      case taee:TypeAlreadyExistsException =>
        // Someone beat us to it! No worries..
      case lee:LimitExceededException =>
        // We've registered too many workflow types!?
        throw new Exception("Can't register new workflow type!", lee)
      //      case onpe:OperationNotPermittedException =>
      //      case ure:UnknownResourceException =>
      //      case ace:AmazonClientException =>
      //      case ase:AmazonServiceException =>
      //      case e:Exception =>

    }
  }
}

class SWFAdapter( _cfg: PropertiesLoader, _splog: Splogger, autoRegister:Boolean = false)
  extends AbstractSWFAdapter
  with SploggerComponent
  with PropertiesLoaderComponent {

  def config = _cfg
  def splog = _splog

  verifyDomain(autoRegister)
  verifyWorkflowType(autoRegister)
}
