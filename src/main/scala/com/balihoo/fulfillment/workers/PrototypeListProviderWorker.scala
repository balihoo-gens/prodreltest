package com.balihoo.fulfillment.workers

import com.amazonaws.services.simpleworkflow.model.{ActivityType, ActivityTask, TaskList}
import com.balihoo.fulfillment.workers.db.{Member, Members}
import com.balihoo.fulfillment.config.WorkflowConfig

object PrototypeListProviderWorkerConfig extends WorkerBaseConfig  {
  val activityType: ActivityType = new ActivityType()
  activityType.setName(WorkflowConfig.properties.getString("LPtaskName"))
  activityType.setVersion(WorkflowConfig.properties.getString("LPtaskVersion"))

  val taskList: TaskList = new TaskList()
  taskList.setName(activityType.getName + "-v" + activityType.getVersion)

  val taskScheduleToClose: Int = WorkflowConfig.properties.getInt("LPtaskScheduleToClose")
  val taskScheduleToStart: Int = WorkflowConfig.properties.getInt("LPtaskScheduleToStart")
  val taskStartToClose: Int = WorkflowConfig.properties.getInt("LPtaskStartToClose")
  val taskHeartbeatTimeout: Int = WorkflowConfig.properties.getInt("LPtaskHeartbeatTimeout")
  val taskDescription: String = WorkflowConfig.properties.getString("LPtaskDescription")

  //worker implementation specific
  val heartbeatFrequency: Int = WorkflowConfig.properties.getInt("LPtaskHeartbeatFrequency")
  val checkResultsFrequency: Int = WorkflowConfig.properties.getInt("LPtaskCheckResultsFrequency")
}

class PrototypeListProviderWorker extends WorkerBase {

  def getConfig: WorkerBaseConfig = PrototypeListProviderWorkerConfig

  def doWork(task: ActivityTask): Unit = {
    respondTaskComplete(task.getTaskToken, "TEST immediate task completion. Input was \n" + task.getInput)
    /* todo: query sqlite
    get inputs
    form query
    perform query against test db
    result to es format (maybe part of query)
    save to disk
    zip it
    s3 it
    return url
     */
  }

  def cancelTask(taskToken: String): Unit = {}
}

//todo: demo async list retrieval


object worker {
  def main(args: Array[String]) {
    val worker: PrototypeListProviderWorker = new PrototypeListProviderWorker
    worker.pollForWorkerTasks()
  }

  def testQuery() = {
    val members: List[Member] = Members.query(-7000, 0)
    println(members.length + " members found")
//    members.foreach(println)
    println("done")
  }



}
