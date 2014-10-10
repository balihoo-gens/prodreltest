package com.balihoo.fulfillment.adapters

import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.{AmazonClientException, AmazonServiceException}

import scala.collection.JavaConversions._

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.balihoo.fulfillment.config._

//for the cake pattern dependency injection
trait DynamoAdapterComponent {
  def dynamoAdapter: AbstractDynamoAdapter with PropertiesLoaderComponent
}

abstract class AbstractDynamoAdapter extends AWSAdapter[AmazonDynamoDBAsyncClient] {
  this: PropertiesLoaderComponent =>
  val mapper = new DynamoDBMapper(client)
  val tableName = config.getString("worker_status_table")
  val readCapacity = config.getOptInt("worker_status_read_capacity", 3)
  val writeCapacity = config.getOptInt("worker_status_write_capacity", 5)

  def put(item:DynamoItem) = {
    def log(s: String) = println(s"DynamoAdapter.put: $s\n$item")
    try {
      client.putItemAsync(item.makeRequest())
    } catch {
      case e:AmazonServiceException =>
        log(e.getMessage)
      case e:AmazonClientException =>
        log(e.getMessage)
      case e:Exception =>
        log(e.getMessage)
    }
  }

  def update(update:DynamoUpdate) = {
    def log(s: String) = println(s"DynamoAdapter.update: $s\n$update")
    try {
      client.updateItemAsync(update.makeRequest())
    } catch {
      case e:AmazonServiceException =>
        log(e.getMessage)
      case e:AmazonClientException =>
        log(e.getMessage)
      case e:Exception =>
        log(e.getMessage)
    }
  }
}

class DynamoAdapter(cfg: PropertiesLoader)
  extends AbstractDynamoAdapter
  with PropertiesLoaderComponent {

  def config = cfg
}

class DynamoItem(table:String) {

  protected val item = collection.mutable.Map[String, AttributeValue]()

  def addNumber(key: String, value: String): DynamoItem = {
    item(key) = new AttributeValue().withN(value)
    this
  }

  def addString(key: String, value: String): DynamoItem = {
    item(key) = new AttributeValue().withS(value)
    this
  }

  def makeRequest() = {
    new PutItemRequest()
      .withTableName(table)
      .withItem(item)
  }

  override def toString = {
    (for((key,value) <- item) yield s"$key: $value").mkString("DynamoItem:{", ", ", "}")
  }
}

class DynamoUpdate(table:String) {

  val updateRequest = new UpdateItemRequest()
    .withTableName(table)

  def forKey(key:String, value:String) = {
    updateRequest.setKey(Map(key -> new AttributeValue().withS(value)))
    this
  }

  def addNumber(key:String, value:String) = {
    updateRequest.addAttributeUpdatesEntry(key,
      new AttributeValueUpdate(new AttributeValue().withN(value), AttributeAction.PUT))
    this
  }

  def addString(key:String, value:String) = {
    updateRequest.addAttributeUpdatesEntry(key,
      new AttributeValueUpdate(new AttributeValue().withS(value), AttributeAction.PUT))
    this
  }

  def makeRequest() = {
    updateRequest
  }
}

