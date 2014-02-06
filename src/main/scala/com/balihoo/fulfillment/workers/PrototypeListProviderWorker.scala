package com.balihoo.fulfillment.workers

import com.amazonaws.services.simpleworkflow.model.{ActivityType, ActivityTask, TaskList}
import play.api.libs.json.{Writes, JsValue, Json}
import scala.slick.driver.SQLiteDriver.simple._
import Database.threadLocalSession
import scala.slick.jdbc.{GetResult, StaticQuery}
import scala.reflect.io.File

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
  case class Member(
    firstname: String,
    lastname: String,
    email: String
  )

  val DB = Database.forURL("jdbc:sqlite:sample.db", driver="org.sqlite.JDBC")

  def getConfig: WorkerBaseConfig = PrototypeListProviderWorkerConfig

  def doWork(task: ActivityTask): Unit = {
    // todo: all
//

    //end test, respond with something
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

  def queryRaw(input: JsValue): List[Member] = {

    val birthday = input \ "birthday" //array of [min, max] offset from current date in days
    val bdenrolled = input \ "bdenrolled" //boolean
    val memstatus = input \ "memstatus" //"active", or something else

    //casts query results to member type
    implicit val getMemberResult = GetResult(r => Member(
      firstname = r.nextString(),
      lastname = r.nextString(),
      email = r.nextString()
    ))
    //note: use r.nextInt, etc, for other types.
    //note: example for optional types
    //      emailunsubscribed = r.nextString() match {
    //        case "" | null => None
    //        case other => Some(DateTime.parse(other))
    //      }

    val bdArray: Array[Int] = birthday.as[Array[Int]]
    val bdMin = bdArray(0)
    val bdMax = bdArray(1)

    //Higher level lifted queries only works for simple operations.
    //Lower level plain SQL queries are necessary for advanced date math.
    //todo: could I maybe pimp the column to add a dateWithoutYearOffset(int) that returns true/false? Then use lifted query
    //There are several ways to do this. See http://slick.typesafe.com/doc/1.0.0/sql.html
    var query = StaticQuery[Member] +
      "select firstname as 'First Name', lastname, email" +
      "\nfrom recipient" +
      "\nwhere (strftime('%J', strftime('%Y', 'now')||strftime('-%m-%d', birthdate)) - strftime('%J', 'now') between " +? bdMin + " and " +? bdMax +
      "\nor strftime('%J', (strftime('%Y', 'now')-1)||strftime('-%m-%d', birthdate)) - strftime('%J', 'now') between " +? bdMin + " and " +? bdMax +
      "\nor strftime('%J', (strftime('%Y', 'now')+1)||strftime('-%m-%d', birthdate)) - strftime('%J', 'now') between " +? bdMin + " and " +? bdMax + ")"

    if (bdenrolled.as[Boolean]) {
      query = query +
        "\nand emailsubscribed < \'now\'" +
        "\nand (emailunsubscribed = '' or emailunsubscribed >= \'now\')"
    }

    //todo: this is for testing, remove before commit
    query = query + "\nlimit 3"
//    println(query2.getStatement)
    //note: nothing like memstatus exists in sample db, ignoring until we have a definition for this

    DB.withSession {
      query.list
    }
  }

  def saveListAsJson(members: List[Member], filename: String) = {
    implicit val memberToJsonWrites: Writes[Member] = Writes[Member](m => Json.obj(
      "First Name" -> m.firstname,
      "Last Name" -> m.lastname,
      "Email" -> m.email
    ))

    val jsonArray = Json.toJson(members)
    println(Json.stringify(jsonArray))

    File(filename).writeAll(Json.stringify(jsonArray))
  }

  def cancelTask(taskToken: String): Unit = {}
}

//todo: demo async list retrieval



object worker extends App {

  //todo: currently testing functions directly. switch back to polling once complete

  val worker = new PrototypeListProviderWorker
  val inputJson = loadJson()
  val jsonOutputFileName = "memberResult.json"

  val members = worker.queryRaw(inputJson \ "target")
  println(members.size + " members found")

  worker.saveListAsJson(members, jsonOutputFileName)


  def loadJson(): JsValue = {
    val inputSource = io.Source.fromFile("testWorkflowInput.json")
    val inputString = inputSource.mkString
    inputSource.close()
    Json.parse(inputString)
  }
}
