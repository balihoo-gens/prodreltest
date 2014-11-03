package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridCreateSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
    with SendGridAdapterComponent =>

  val _cfg: PropertiesLoader

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("participantId", "string", "The participant ID used to identify the SendGrid subaccount"),
      new ActivityParameter("useTestSubaccount", "boolean", "True if the SendGrid test account should be used"),
      new ActivityParameter("firstName", "string", "The first name from the participant's marketing address"),
      new ActivityParameter("lastName", "string", "The last name from the participant's marketing address"),
      new ActivityParameter("address", "string", "The street address from the participant's marketing address"),
      new ActivityParameter("city", "string", "The city from the participant's marketing address"),
      new ActivityParameter("state", "string", "The state from the participant's marketing address"),
      new ActivityParameter("zip", "string", "The zip from the participant's marketing address"),
      new ActivityParameter("country", "string", "The country from the participant's marketing address"),
      new ActivityParameter("phone", "string", "The phone number from the participant's marketing address")
    ), new ActivityResult("string", "The subaccount ID"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      val subaccountId = SendGridSubaccountId(params("participantId"), params("useTestSubaccount").toBoolean)
      val credentials = sendGridAdapter.subaccountToCredentials(subaccountId)
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
      sendGridAdapter.createSubaccount(subaccount)
    }
  }
}

class SendGridCreateSubaccount(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractSendGridCreateSubaccount
  with LoggingWorkflowAdapterImpl
  with SendGridAdapterComponent {

  private lazy val _sendGridAdapter = new SendGridAdapter(_cfg, _splog)
  def sendGridAdapter = _sendGridAdapter
}

object sendgrid_createsubaccount extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new SendGridCreateSubaccount(cfg, splog)
  }
}
