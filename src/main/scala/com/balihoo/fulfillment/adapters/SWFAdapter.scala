package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.config._

import scala.concurrent.Promise
import scala.concurrent.ExecutionContext.Implicits.global

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: AbstractSWFAdapter with PropertiesLoaderComponent
}

abstract class AbstractSWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] {
  this: PropertiesLoaderComponent =>

  protected val _taskListName = new SWFName(name+version)
  protected val _name = new SWFName(swfAdapter.config.getString("name"))
  protected val _version = new SWFVersion(swfAdapter.config.getString("version"))
  protected val _taskList: TaskList = new TaskList().withName(taskListName)

  protected val taskReq: PollForActivityTaskRequest = new PollForActivityTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)
  protected val countReq: CountPendingActivityTaskRequest = new CountPendingActivityTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  def taskListName = _taskListName
  def name = _name
  def version = _version
  def taskList = _taskList

  def getTask(): Future[ActivityTask]  = {
    val taskPromise = Promise[ActivityTask]()

    object activityPollHandler extends AsyncHandler[PollForActivityTaskRequest, ActivityTask] {
      override def onSuccess(req:PollForActivityTaskRequest, task:ActivityTask) {
        taskPromise.success(task)
      }
      override def onError(e:Exception) {
        taskPromise.failure(e)
      }
    }

    object activityCountHandler extends AsyncHandler[CountPendingActivityTaskRequest, PendingTaskCount] {
      override def onSuccess(req:CountPendingActivityTaskRequest, count:PendingTaskCount) {
        if (count.getCount > 0) {
          swfAdapter.client.pollForActivityTaskAsync(taskReq)
        }
      }
      override def onError(e:Exception) {
        taskPromise.failure(e)
      }
    }

    swfAdapter.client.countPendingActivityTasksAsync(countReq, activityCountHandler)

    p.future
  }
}

class SWFAdapter(cfg: PropertiesLoader) extends AbstractSWFAdapter with PropertiesLoaderComponent {
  def config = cfg
}
