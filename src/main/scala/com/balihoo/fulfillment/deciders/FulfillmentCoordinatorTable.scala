package com.balihoo.fulfillment.deciders

import scala.language.implicitConversions
import scala.collection.JavaConversions._

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride
import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model._
import org.joda.time.{Minutes, DateTime}

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.util._

import play.api.libs.json._

class FulfillmentCoordinatorTable {
  this: DynamoAdapterComponent
    with SploggerComponent =>

  val tableName = dynamoAdapter.config.getString("coordinator_status_table")
  val readCapacity = dynamoAdapter.config.getOrElse("coordinator_status_read_capacity", 3)
  val writeCapacity = dynamoAdapter.config.getOrElse("coordinator_status_write_capacity", 5)

  waitForActiveTable()

  def waitForActiveTable() = {

    var active = false
    while(!active) {
      try {
        splog.info(s"Checking for coordinator status table $tableName")
        val tableDesc = dynamoAdapter.client.describeTable(tableName)

        // I didn't see any constants for these statuses..
        tableDesc.getTable.getTableStatus match {
          case "CREATING" =>
            splog.info("Coordinator status table is being created. Let's wait a while")
            Thread.sleep(5000)
          case "UPDATING" =>
            splog.info("Coordinator status table is being updated. Let's wait a while")
            Thread.sleep(5000)
          case "DELETING" =>
            val errstr = "The coordinator status table is being deleted!"
            splog.error(errstr)
            throw new Exception(s"ERROR! $errstr")
          case "ACTIVE" =>
            splog.info("Coordinator status table is active")
            active = true
        }
      } catch {
        case rnfe:ResourceNotFoundException =>
          splog.warning(s"Table not found! Creating it!")
          createCoordinatorTable()
      }
    }
  }

  def createCoordinatorTable() = {
    val ctr = new CreateTableRequest()
    ctr.setTableName(tableName)
    ctr.setProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity))
    ctr.setAttributeDefinitions(List(new AttributeDefinition("instance", "S")))
    ctr.setKeySchema(List( new KeySchemaElement("instance", "HASH")))
    try {
      dynamoAdapter.client.createTable(ctr)
    } catch {
      case e:Exception =>
        splog.error("Error creating coordinator table: " + e.getMessage)
    }
  }

  def insert(entry:FulfillmentCoordinatorEntry) = {
    dynamoAdapter.put(entry.getDynamoItem)
  }

  def update(entry:FulfillmentCoordinatorEntry) = {
    dynamoAdapter.update(entry.getDynamoUpdate)
  }

  def get() = {
    val scanExp:DynamoDBScanExpression = new DynamoDBScanExpression()

    val oldest = DateTime.now.minusDays(1)

    scanExp.addFilterCondition("last",
      new Condition()
        .withComparisonOperator(ComparisonOperator.GT)
        .withAttributeValueList(new AttributeValue().withS(UTCFormatter.format(oldest))))

    val list = dynamoAdapter.mapper.scan(classOf[FulfillmentCoordinatorEntry], scanExp,
      new DynamoDBMapperConfig(new TableNameOverride(tableName)))
    for(coordinator:FulfillmentCoordinatorEntry <- list) {
      coordinator.minutesSinceLast = Minutes.minutesBetween(DateTime.now, new DateTime(coordinator.last)).getMinutes
    }

    list.toList
  }

}

@DynamoDBTable(tableName="_CONFIGURED_IN_COORDINATOR_PROPERTIES_")
class FulfillmentCoordinatorEntry() {
  var instance:String = ""
  var hostAddress:String = ""
  var domain:String = ""
  var workflowName:String = ""
  var workflowVersion:String = ""
  var specification:String = ""
  var status:String = ""
  var resolutionHistory:String = ""
  var start:String = ""
  var last:String = ""

  var minutesSinceLast:Long = 0

  var tableName:String = "_MUST_BE_SET_"

  def toJson:JsValue = {
    Json.obj(
      "instance" -> instance,
      "hostAddress" -> hostAddress,
      "domain" -> domain,
      "workflowName" -> workflowName,
      "workflowVersion" -> workflowVersion,
      "specification" -> specification,
      "status" -> status,
      "resolutionHistory" -> resolutionHistory,
      "start" -> start,
      "last" -> last,
      "minutesSinceLast" -> minutesSinceLast
    )

  }

  @DynamoDBHashKey(attributeName="instance")
  def getInstance():String = { instance }
  def setInstance(ins:String) = { this.instance = ins }

  @DynamoDBHashKey(attributeName="hostAddress")
  def getHostAddress:String = { hostAddress }
  def setHostAddress(ha:String) = { this.hostAddress = ha }

  @DynamoDBAttribute(attributeName="domain")
  def getDomain:String = { domain }
  def setDomain(domain:String) { this.domain = domain; }

  @DynamoDBAttribute(attributeName="workflowName")
  def getWorkflowName:String = { workflowName }
  def setWorkflowName(workflowName:String) { this.workflowName = workflowName; }

  @DynamoDBAttribute(attributeName="workflowVersion")
  def getWorkflowVersion:String = { workflowVersion }
  def setWorkflowVersion(workflowVersion:String) { this.workflowVersion = workflowVersion; }

  @DynamoDBAttribute(attributeName="specification")
  def getSpecification:String = { specification }
  def setSpecification(specification:String) { this.specification = specification; }

  @DynamoDBAttribute(attributeName="status")
  def getStatus:String = { status }
  def setStatus(status:String) { this.status = status; }

  @DynamoDBAttribute(attributeName="resolutionHistory")
  def getResolutionHistory:String = { resolutionHistory }
  def setResolutionHistory(resolutionHistory:String) { this.resolutionHistory = resolutionHistory; }

  @DynamoDBAttribute(attributeName="start")
  def getStart:String = { start }
  def setStart(start:String) { this.start = start; }

  @DynamoDBAttribute(attributeName="last")
  def getLast:String = { last }
  def setLast(last:String) { this.last = last; }

  def getDynamoItem:DynamoItem = {
    new DynamoItem(tableName)
      .addString("instance", instance)
      .addString("hostAddress", hostAddress)
      .addString("domain", domain)
      .addString("workflowName", workflowName)
      .addString("workflowVersion", workflowVersion)
      .addString("specification", specification)
      .addString("status", status)
      .addString("resolutionHistory", resolutionHistory)
      .addString("start", start)
      .addString("last", last)
  }

  def getDynamoUpdate:DynamoUpdate = {
    new DynamoUpdate(tableName)
      .forKey("instance", instance)
      .addString("status", status)
      .addString("resolutionHistory", resolutionHistory)
      .addString("last", last)
  }
}
