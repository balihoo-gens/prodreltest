package com.balihoo.fulfillment.workers.sendgrid

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridCreateSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
    with SendGridAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringParameter("participantId", "The participant ID used to identify the SendGrid subaccount"),
      new BooleanParameter("useTestSubaccount", "True if the SendGrid test account should be used"),
      new StringParameter("firstName", "The first name from the participant's marketing address"),
      new StringParameter("lastName", "The last name from the participant's marketing address"),
      new StringParameter("address", "The street address from the participant's marketing address"),
      new StringParameter("city", "The city from the participant's marketing address"),
      new StringParameter("state", "The state from the participant's marketing address"),
      new StringParameter("zip", "The zip from the participant's marketing address"),
      new StringParameter("country", "The country from the participant's marketing address"),
      new StringParameter("phone", "The phone number from the participant's marketing address")
    ), new StringResultType("The subaccount username"))
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    val subaccountId = SendGridSubaccountId(args[String]("participantId"), args[Boolean]("useTestSubaccount"))
    val credentials = sendGridAdapter.subaccountToCredentials(subaccountId)
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
    sendGridAdapter.createSubaccount(subaccount)

    // Configuration stuff that can be done at account creation time.  (This stuff won't need to change later.)
    sendGridAdapter.activateApp(credentials.apiUser, "eventnotify")
    sendGridAdapter.activateApp(credentials.apiUser, "clicktrack")
    sendGridAdapter.activateApp(credentials.apiUser, "opentrack")
    sendGridAdapter.activateApp(credentials.apiUser, "subscriptiontrack")

    getSpecification.createResult(credentials.apiUser)
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
