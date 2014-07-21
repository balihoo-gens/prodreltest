package com.balihoo.fulfillment.adapters

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper
import com.amazonaws.{AmazonClientException, AmazonServiceException}

import scala.collection.JavaConversions._

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBClient, AmazonDynamoDBAsyncClient}
import com.amazonaws.services.dynamodbv2.model._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.regions.{Regions, Region}

class DynamoAdapter(loader: PropertiesLoader) extends AWSAdapter[AmazonDynamoDBAsyncClient](loader){
//  val syncclient = new AmazonDynamoDBClient(credentials)
  client.setRegion(Region.getRegion(Regions.DEFAULT_REGION))
  val mapper = new DynamoDBMapper(client)

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

  override def toString() = {
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
