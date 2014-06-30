package com.balihoo.fulfillment

import com.amazonaws.{AmazonClientException, AmazonServiceException}

import scala.collection.JavaConversions._

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model.{PutItemRequest, AttributeValue}
import com.balihoo.fulfillment.config.PropertiesLoader

class DynamoAdapter(loader: PropertiesLoader) {
  private val accessKey: String = loader.getString("aws.accessKey")
  private val secretKey = loader.getString("aws.secretKey")

  private val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val client = new AmazonDynamoDBAsyncClient(credentials)

  val config = loader

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