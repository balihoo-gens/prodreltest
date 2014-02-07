package com.balihoo.fulfillment.workers

import com.amazonaws.services.simpleworkflow.model.{ActivityType, ActivityTask, TaskList}
import play.api.libs.json.{Writes, JsValue, Json}
import scala.slick.driver.SQLiteDriver.simple._
import Database.threadLocalSession
import scala.slick.jdbc.{GetResult, StaticQuery}
import com.balihoo.fulfillment.config.WorkflowConfig
import java.io.File
import com.amazonaws.services.s3.model.{GetObjectRequest, PutObjectResult}


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
  val s3Bucket: String = "balihoo.dev.fulfillment"
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

    //todo: exception handling.

    val inputJson = Json.parse(task.getInput)

    /* todo: determine file naming and format conventions, depending on how it will be pulled in for ES
    This worker doesn't have any concept of an order. It just pulls recipients based on target info.
    So determine if some meaningful unique filename can be created, or just use random.
     */
    val jsonOutputFileName = "membersResult.json"
    val zipOutputFile = File.createTempFile("listProviderResult_", ".zip")
    zipOutputFile.deleteOnExit() // in case we die before cleanup
    println("creating temp file at " + zipOutputFile.getAbsolutePath)

    val members = query(inputJson)

    val jsonString = memberListToJsonString(members)
    zipAndSaveJson(jsonString, jsonOutputFileName, zipOutputFile.getAbsolutePath)

    val url = uploadToS3(zipOutputFile)

    zipOutputFile.delete()

    respondTaskComplete(task.getTaskToken, url)
  }

  /**
   * Perform query against the sqlite database and cast the result into Member objects
   * @param input JsValue with keys for targeting parameters
   *              birthday - array of 2 int [min, max], which are min and max days offset from current date in days. eg: [-7, 0]
   *              bdenrolled - boolean that represents whether the member has enrolled in birthday emails.
   * @return List of Member objects
   */
  def query(input: JsValue): List[Member] = {

    val birthday = input \ "birthday"
    val bdenrolled = input \ "bdenrolled"
//    val memstatus = input \ "memstatus"

    //casts query results to member type
    implicit val getMemberResult = GetResult(r => Member(
      firstname = r.nextString(),
      lastname = r.nextString(),
      email = r.nextString()
    ))
    //note: use r.nextInt etc for other types, or use r.<< for inferred type based on assign value
    //note: example for optional types
    //      emailunsubscribed = r.nextString() match {
    //        case "" | null => None
    //        case other => Some(DateTime.parse(other))
    //      }

    val bdArray = birthday.as[Array[Int]]
    val bdMin = bdArray(0)
    val bdMax = bdArray(1)

    //Higher level lifted queries only work for simple operations.
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

    //for testing, limit resut set
    //query = query + "\nlimit 3"

    //note: nothing like memstatus exists in sample db, ignoring until we have a definition for this

    DB.withSession {
      //note: query.list pulls the whole result set into memory. For production code with potentially
      //huge result sets, we will want to iterate through results in chunks
      //Possibly stream from DB -> zip -> s3
      query.list
    }
  }

  /**
   * Convert a list of member objects into a Json string
   * @param members
   * @return
   */
  def memberListToJsonString(members: List[Member]) = {
    implicit val memberToJsonWrites = Writes[Member](m => Json.obj(
      "First Name" -> m.firstname,
      "Last Name" -> m.lastname,
      "Email" -> m.email
    ))

    val jsonArray = Json.toJson(members)
    Json.stringify(jsonArray)
  }

  /**
   * Save a string to a file in a zip archive on disk
   * @param jsonString
   * @param jsonFilename
   * @param zipFilename
   */
  def zipAndSaveJson(jsonString: String, jsonFilename: String, zipFilename: String) = {

    import java.util.zip.{ZipEntry, ZipOutputStream}
    import java.io.FileOutputStream

    val zip = new ZipOutputStream(new FileOutputStream(zipFilename))

    zip.putNextEntry(new ZipEntry(jsonFilename))
    zip.write(jsonString.getBytes)
    zip.closeEntry()
    zip.close()
  }

  /**
   * Uploads a file on disk to s3
   * @param fileToUpload
   * @return the url to the hosted file
   */
  def uploadToS3(fileToUpload: File): String = {
    import com.amazonaws.services.s3.AmazonS3Client
    val s3Bucket = PrototypeListProviderWorkerConfig.s3Bucket
    val s3Key = fileToUpload.getName

    val s3client = new AmazonS3Client(WorkflowConfig.creds)
    s3client.putObject(s3Bucket, s3Key, fileToUpload)
    //todo: access control list depending on what will be reading this.

    s3client.getResourceUrl(s3Bucket, s3Key)
  }

  def cancelTask(taskToken: String): Unit = {}
}

//todo: demo async list retrieval



object worker extends App {

  val worker = new PrototypeListProviderWorker
  worker.pollForWorkerTasks()

  //for testing json against worker methods directly, without retrieving from workflow
  def loadJson(): JsValue = {
    val inputSource = io.Source.fromFile("testWorkflowInput.json")
    val inputString = inputSource.mkString
    inputSource.close()
    Json.parse(inputString)
  }
}
