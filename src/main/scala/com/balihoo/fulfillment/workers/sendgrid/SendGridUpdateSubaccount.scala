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

  override def handleTask(args: ActivityArgs):ActivityResult = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    val subaccountUser = args[String]("subaccount")
    val webhookUrl = args[URI]("webhookUrl").toURL
    val webhookUsername = args[String]("webhookUsername")
    val webhookPassword = args[String]("webhookPassword")
    val ipAddress = args[String]("ipAddress")
    val whitelabel = args[String]("whitelabel")
    val credentials = sendGridAdapter.getCredentials(subaccountUser)
    val subaccount = new SendGridSubaccount(
      _credentials = credentials,
      _firstName = args[String]("firstName"),
      _lastName = args[String]("lastName"),
      _address = args[String]("address"),
      _city = args[String]("city"),
      _state = args[String]("state"),
      _zip = args[String]("zip"),
      _country = args[String]("country"),
      _phone = args[String]("phone"))
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
