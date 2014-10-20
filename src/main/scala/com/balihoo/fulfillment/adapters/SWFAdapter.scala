package com.balihoo.fulfillment.adapters

import com.amazonaws.{AmazonServiceException, AmazonClientException}
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model._
import com.amazonaws.handlers.AsyncHandler

import scala.concurrent.{Promise, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: AbstractSWFAdapter with PropertiesLoaderComponent
}

abstract class AbstractSWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] {
  this: PropertiesLoaderComponent
    with SploggerComponent =>

  private lazy val _name = new SWFName(config.getString("name"))
  private lazy val _version = new SWFVersion(config.getString("version"))
  private lazy val _taskListName = new SWFName(_name + _version)
  private lazy val _taskList: TaskList = new TaskList().withName(_taskListName)
  private lazy val _workflowName = new SWFName(config.getString("workflowName"))
  private lazy val _workflowVersion = new SWFVersion(config.getString("workflowVersion"))
  private lazy val _workflowTaskListName = new SWFName(workflowName+workflowVersion)

  //longpoll by default, unless config says "longpoll=false"
  protected val _longPoll = config.getOrElse("longpoll",true)

  def taskListName = _taskListName
  def name = _name
  def version = _version
  def taskList = _taskList
  def workflowName = _workflowName
  def workflowVersion = _workflowVersion
  def workflowTaskListName = _workflowTaskListName

  protected val taskReq: PollForActivityTaskRequest = new PollForActivityTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)
  protected val countReq: CountPendingActivityTasksRequest = new CountPendingActivityTasksRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  def getTask(): Future[Option[ActivityTask]]  = {
    val taskPromise = Promise[Option[ActivityTask]]()

    object activityPollHandler extends AsyncHandler[PollForActivityTaskRequest, ActivityTask] {
      override def onSuccess(req:PollForActivityTaskRequest, task:ActivityTask) {
        if (task != null && task.getTaskToken != null) {
          taskPromise.success(Some(task))
        } else {
          taskPromise.success(None)
        }
      }
      override def onError(e:Exception) {
        taskPromise.failure(e)
      }
    }

    object activityCountHandler extends AsyncHandler[CountPendingActivityTasksRequest, PendingTaskCount] {
      override def onSuccess(req:CountPendingActivityTasksRequest, count:PendingTaskCount) {
        if (count.getCount > 0) {
          client.pollForActivityTaskAsync(taskReq, activityPollHandler)
        } else {
          Thread.sleep(1000)
          taskPromise.success(None)
        }
      }
      override def onError(e:Exception) {
        taskPromise.failure(e)
      }
    }

    if (_longPoll) {
      client.pollForActivityTaskAsync(taskReq, activityPollHandler)
    } else {
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
    val wt = new WorkflowType
    wt.setName(workflowName)
    wt.setVersion(workflowVersion)
    val wtr = new DescribeWorkflowTypeRequest
    wtr.setDomain(domain)
    wtr.setWorkflowType(wt)

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
    val taskList = new TaskList
    taskList.setName(workflowTaskListName)
    val rwtr = new RegisterWorkflowTypeRequest
    rwtr.setDomain(domain)
    rwtr.setName(workflowName)
    rwtr.setVersion(workflowVersion)
    rwtr.setDefaultTaskList(taskList)

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

class SWFAdapter(cfg: PropertiesLoader, _splog: Splogger, autoRegister:Boolean = false)
  extends AbstractSWFAdapter
  with SploggerComponent
  with PropertiesLoaderComponent {

  def config = cfg
  def splog = _splog

  verifyDomain(autoRegister)
  verifyWorkflowType(autoRegister)
}
