package com.balihoo.fulfillment

import com.amazonaws.{AmazonClientException, AmazonServiceException}

import scala.collection.JavaConversions._

import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model.{PutItemRequest, AttributeValue}

class DynamoAdapter(loader: PropertiesLoader) extends AWSAdapter[AmazonDynamoDBAsyncClient](loader) {
  def putItem(item:DynamoItem) = {
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
}

class DynamoItem(table:String) {

  protected var item = collection.mutable.Map[String, AttributeValue]()

  def addNumber(key:String, value:String):DynamoItem = {
    item += (key -> new AttributeValue().withN(value))
    this
  }

  def addString(key:String, value:String):DynamoItem = {
    item += (key -> new AttributeValue().withS(value))
    this
  }

  def makeRequest() = {
    new PutItemRequest()
      .withTableName(table)
      .withItem(item)
  }
}
