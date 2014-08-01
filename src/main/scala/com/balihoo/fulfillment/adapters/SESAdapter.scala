package com.balihoo.fulfillment.adapters

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.balihoo.fulfillment.config._
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceAsyncClient
import com.amazonaws.services.simpleemail.model.{
  VerifyEmailIdentityRequest,
  SendEmailRequest,
  ListIdentitiesRequest,
  Destination,
  Message,
  Body,
  Content
}

trait SESAdapterComponent {
  def sesAdapter: SESAdapter with PropertiesLoaderComponent
}

abstract class SESAdapter extends AWSAdapter[AmazonSimpleEmailServiceAsyncClient] {
  this: PropertiesLoaderComponent =>

  def verifyEmailAddress(address: String): String  = {
    val request = new VerifyEmailIdentityRequest()
    request.setEmailAddress(address)
    val result = client.verifyEmailIdentity(request)
    result.toString
  }

  def listVerifiedEmailAddresses(): List[String] = {
    val request = new ListIdentitiesRequest()
    request.setIdentityType("EmailAddress")
    val result = client.listIdentities(request)
    result.getIdentities.asScala.toList
  }

  def sendEmail(from: String, recipients: List[String], subject: String, body: String, html: Boolean = true): String = {
    val msgbody = new Body()
    if (html) {
      msgbody.setHtml(new Content(body))
    } else {
      msgbody.setText(new Content(body))
    }
    val rcpts = new Destination(recipients.asJava)
    val message = new Message(new Content(subject), msgbody)
    val request = new SendEmailRequest(from, rcpts, message)
    val result = client.sendEmail(request)
    result.getMessageId
  }
}

object SESAdapter {
  def apply(cfg: PropertiesLoader) = {
    new SESAdapter with PropertiesLoaderComponent { def config = cfg }
  }
}
