package com.balihoo.fulfillment.workers

import java.net.URI

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridUpdateSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
    with SendGridAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringActivityParameter("subaccount", "The SendGrid subaccount username"),
      new StringActivityParameter("firstName", "The first name from the participant's marketing address"),
      new StringActivityParameter("lastName", "The last name from the participant's marketing address"),
      new StringActivityParameter("address", "The street address from the participant's marketing address"),
      new StringActivityParameter("city", "The city from the participant's marketing address"),
      new StringActivityParameter("state", "The state from the participant's marketing address"),
      new StringActivityParameter("zip", "The zip from the participant's marketing address"),
      new StringActivityParameter("country", "The country from the participant's marketing address"),
      new StringActivityParameter("phone", "The phone number from the participant's marketing address"),
      new UriActivityParameter("webhookUrl", "The event notification webhook URL"),
      new StringActivityParameter("webhookUsername", "The event notification webhook username"),
      new EncryptedActivityParameter("webhookPassword", "The even notification webhook password"),
      new Ipv4ActivityParameter("ipAddress", "The IP address SendGrid should use for sending email"),
      new StringActivityParameter("whitelabel", "The SendGrid whitelabel for the subaccount")
    ), new StringActivityResult("The subaccount name"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      val subaccountUser = params("subaccount")
      val webhookUrl = params[URI]("webhookUrl").toURL
      val webhookUsername = params("webhookUsername")
      val webhookPassword = params("webhookPassword")
      val ipAddress = params("ipAddress")
      val whitelabel = params("whitelabel")
      val credentials = sendGridAdapter.getCredentials(subaccountUser)
      val subaccount = new SendGridSubaccount(
        _credentials = credentials,
        _firstName = params("firstName"),
        _lastName = params("lastName"),
        _address = params("address"),
        _city = params("city"),
        _state = params("state"),
        _zip = params("zip"),
        _country = params("country"),
        _phone = params("phone"))
      sendGridAdapter.updateProfile(subaccount)
      sendGridAdapter.configureEventNotificationApp(subaccountUser, webhookUrl, webhookUsername, webhookPassword)
      sendGridAdapter.setIpAddress(subaccountUser, ipAddress)
      sendGridAdapter.setWhitelabel(subaccountUser, whitelabel)
      subaccountUser
    }
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
