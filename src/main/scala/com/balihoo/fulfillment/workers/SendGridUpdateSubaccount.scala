package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridUpdateSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
    with SendGridAdapterComponent =>

  val _cfg: PropertiesLoader

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("subaccount", "string", "The SendGrid subaccount username"),
      new ActivityParameter("firstName", "string", "The first name from the participant's marketing address"),
      new ActivityParameter("lastName", "string", "The last name from the participant's marketing address"),
      new ActivityParameter("address", "string", "The street address from the participant's marketing address"),
      new ActivityParameter("city", "string", "The city from the participant's marketing address"),
      new ActivityParameter("state", "string", "The state from the participant's marketing address"),
      new ActivityParameter("zip", "string", "The zip from the participant's marketing address"),
      new ActivityParameter("country", "string", "The country from the participant's marketing address"),
      new ActivityParameter("phone", "string", "The phone number from the participant's marketing address"),
      new ActivityParameter("webhookUrl", "string", "The event notification webhook URL"),
      new ActivityParameter("webhookUsername", "string", "The event notification webhook username"),
      new ActivityParameter("webhookPassword", "string", "The even notification webhook password", sensitive = true),
      new ActivityParameter("ipAddress", "string", "The IP address SendGrid should use for sending email"),
      new ActivityParameter("whitelabel", "string", "The SendGrid whitelabel for the subaccount")
    ), new ActivityResult("string", "The subaccount name"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      val subaccountUser = params("subaccount")
      val webhookUrl = params("webhookUrl")
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
