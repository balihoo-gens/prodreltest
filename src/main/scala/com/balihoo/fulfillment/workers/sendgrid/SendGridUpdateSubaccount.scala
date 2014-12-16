package com.balihoo.fulfillment.workers.sendgrid

import java.net.URI

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridUpdateSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
    with SendGridAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringParameter("subaccount", "The SendGrid subaccount username"),
      new StringParameter("firstName", "The first name from the participant's marketing address"),
      new StringParameter("lastName", "The last name from the participant's marketing address"),
      new StringParameter("address", "The street address from the participant's marketing address"),
      new StringParameter("city", "The city from the participant's marketing address"),
      new StringParameter("state", "The state from the participant's marketing address"),
      new StringParameter("zip", "The zip from the participant's marketing address"),
      new StringParameter("country", "The country from the participant's marketing address"),
      new StringParameter("phone", "The phone number from the participant's marketing address"),
      new UriParameter("webhookUrl", "The event notification webhook URL"),
      new StringParameter("webhookUsername", "The event notification webhook username"),
      new EncryptedParameter("webhookPassword", "The even notification webhook password"),
      new Ipv4Parameter("ipAddress", "The IP address SendGrid should use for sending email"),
      new StringParameter("whitelabel", "The SendGrid whitelabel for the subaccount")
    ), new StringResultType("The subaccount name"))
  }

  override def handleTask(params: ActivityArgs):ActivityResult = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    val subaccountUser = params[String]("subaccount")
    val webhookUrl = params[URI]("webhookUrl").toURL
    val webhookUsername = params[String]("webhookUsername")
    val webhookPassword = params[String]("webhookPassword")
    val ipAddress = params[String]("ipAddress")
    val whitelabel = params[String]("whitelabel")
    val credentials = sendGridAdapter.getCredentials(subaccountUser)
    val subaccount = new SendGridSubaccount(
      _credentials = credentials,
      _firstName = params[String]("firstName"),
      _lastName = params[String]("lastName"),
      _address = params[String]("address"),
      _city = params[String]("city"),
      _state = params[String]("state"),
      _zip = params[String]("zip"),
      _country = params[String]("country"),
      _phone = params[String]("phone"))
    sendGridAdapter.updateProfile(subaccount)
    sendGridAdapter.configureEventNotificationApp(subaccountUser, webhookUrl, webhookUsername, webhookPassword)
    sendGridAdapter.setIpAddress(subaccountUser, ipAddress)
    sendGridAdapter.setWhitelabel(subaccountUser, whitelabel)

    getSpecification.createResult(subaccountUser)
  }
}

class SendGridUpdateSubaccount(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractSendGridUpdateSubaccount
  with LoggingWorkflowAdapterImpl
  with SendGridAdapterComponent {

  private lazy val _sendGridAdapter = new SendGridAdapter(_cfg, _splog)
  def sendGridAdapter = _sendGridAdapter
}

object sendgrid_updatesubaccount extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new SendGridUpdateSubaccount(cfg, splog)
  }
}
