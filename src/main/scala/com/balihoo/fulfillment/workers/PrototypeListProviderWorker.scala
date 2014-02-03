package com.balihoo.fulfillment.workers

import com.amazonaws.services.simpleworkflow.model.{ActivityType, ActivityTask, TaskList}
import com.balihoo.fulfillment.models.{Member, Members}
import com.balihoo.fulfillment.config.WorkflowConfig

object PrototypeListProviderWorkerConfig extends WorkerBaseConfig  {
  val activityType: ActivityType = new ActivityType()
  activityType.setName("PrototypeLPActivityType")
  activityType.setVersion("0.1")

  val taskList: TaskList = new TaskList()
  taskList.setName(activityType.getName + "-v" + activityType.getVersion)

  val taskScheduleToClose: Int = 300
  val taskScheduleToStart: Int = 70
  val taskStartToClose: Int = 200
  val taskHeartbeatTimeout: Int = 5
  val taskDescription: String = "Gets a list of recipients from a Sqlite database using input criteria"

  //worker implementation specific
  val heartbeatFrequency: Int = 3
  val checkResultsFrequency: Int = 12
}

class PrototypeListProviderWorker extends WorkerBase {

  def getConfig: WorkerBaseConfig = PrototypeListProviderWorkerConfig

  def doWork(task: ActivityTask): Unit = {
    respondTaskComplete(task.getTaskToken, "TEST immediate task completion")
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
//    testDDL()
  }

  def testDDL() = {
    Members.ddl.createStatements.foreach(println)
  }

  def testQuery() = {
    val members: List[Member] = Members.query(-7000, 0)
    println(members.length + " members found")
//    members.foreach(println)
    println("done")
  }



}
