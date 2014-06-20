package com.balihoo.fulfillment

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceAsyncClient
import com.amazonaws.services.simpleemail.model.{
  VerifyEmailAddressRequest,
  SendEmailRequest,
  ListIdentitiesRequest,
  Destination,
  Message,
  Body,
  Content
}

class SESAdapter(loader: PropertiesLoader) {
  private val accessKey: String = loader.getString("aws.accessKey")
  private val secretKey = loader.getString("aws.secretKey")

  private val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val client = new AmazonSimpleEmailServiceAsyncClient(credentials)

  val config = loader

  def verifyEmailAddress(address: String) = {
    var request = new VerifyEmailAddressRequest()
    request.setEmailAddress(address)
    client.verifyEmailAddress(request)
  }

  def listVerifiedEmailAddresses(): List[String] = {
    var request = new ListIdentitiesRequest()
    request.setIdentityType("EmailAddress")
    val result = client.listIdentities(request)
    result.getIdentities().asScala.toList
  }

  def sendEmail(from: String, recipients: List[String], subject: String, body: String): String = {
    val msgbody = new Body(new Content(body))
    val rcpts = new Destination(recipients.asJava)
    val message = new Message(new Content(subject), msgbody)
    val request = new SendEmailRequest(from, rcpts, message)
    val result = client.sendEmail(request)
    println(result.toString)
    result.getMessageId()
  }
}
