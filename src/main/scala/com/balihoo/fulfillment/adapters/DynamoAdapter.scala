package com.balihoo.fulfillment.adapters

import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.{AmazonClientException, AmazonServiceException}

import scala.collection.JavaConversions._

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.balihoo.fulfillment.config.PropertiesLoader

class DynamoAdapter(loader: PropertiesLoader) extends AWSAdapter[AmazonDynamoDBAsyncClient](loader){
  val mapper = new DynamoDBMapper(client)

  def put(item:DynamoItem) = {
    try {
      client.putItemAsync(item.makeRequest())
    } catch {
      case e:AmazonServiceException =>
        println(e.getMessage)
      case e:AmazonClientException =>
        println(e.getMessage)
      case e:Exception =>
        println(e.getMessage)
    }
  }

  def update(update:DynamoUpdate) = {
    try {
      client.updateItemAsync(update.makeRequest())
    } catch {
      case e:AmazonServiceException =>
        println(e.getMessage)
      case e:AmazonClientException =>
        println(e.getMessage)
      case e:Exception =>
        println(e.getMessage)
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

