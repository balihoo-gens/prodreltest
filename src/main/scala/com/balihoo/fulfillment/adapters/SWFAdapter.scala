package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model._
import com.amazonaws.handlers.AsyncHandler

import scala.concurrent.{Promise, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import com.balihoo.fulfillment.config._

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: AbstractSWFAdapter with PropertiesLoaderComponent
}

abstract class AbstractSWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] {
  this: PropertiesLoaderComponent =>

  protected lazy val _taskListName = new SWFName(name+version)
  protected lazy val _name = new SWFName(config.getString("name"))
  protected lazy val _version = new SWFVersion(config.getString("version"))
  protected lazy val _taskList: TaskList = new TaskList().withName(taskListName)

  protected val _longPoll = config.getOrElse("longpoll",false)

  protected val taskReq: PollForActivityTaskRequest = new PollForActivityTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)
  protected val countReq: CountPendingActivityTasksRequest = new CountPendingActivityTasksRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  def taskListName = _taskListName
  def name = _name
  def version = _version
  def taskList = _taskList

  def getTask(): Future[Option[ActivityTask]]  = {
    val taskPromise = Promise[Option[ActivityTask]]()

    object activityPollHandler extends AsyncHandler[PollForActivityTaskRequest, ActivityTask] {
      override def onSuccess(req:PollForActivityTaskRequest, task:ActivityTask) {
        taskPromise.success(if (task != null) Some(task) else None)
      }
      override def onError(e:Exception) {
        taskPromise.failure(e)
      }
    }

    object activityCountHandler extends AsyncHandler[CountPendingActivityTasksRequest, PendingTaskCount] {
      override def onSuccess(req:CountPendingActivityTasksRequest, count:PendingTaskCount) {
        if (count.getCount > 0) {
          client.pollForActivityTaskAsync(taskReq)
        } else {
          Thread.sleep(100)
          taskPromise.success(None)
        }
      }
      override def onError(e:Exception) {
        taskPromise.failure(e)
      }
    }

    if (_longPoll) {
      client.pollForActivityTaskAsync(taskReq)
    } else {
      client.countPendingActivityTasksAsync(countReq, activityCountHandler)
    }

    taskPromise.future
  }
}

class SWFAdapter(cfg: PropertiesLoader) extends AbstractSWFAdapter with PropertiesLoaderComponent {
  def config = cfg
}
