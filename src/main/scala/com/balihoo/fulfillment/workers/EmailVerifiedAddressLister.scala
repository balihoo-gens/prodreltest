package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractEmailVerifiedAddressLister extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SESAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(), new StringsResultType("Array of email addresses"))
  }

  override def handleTask(params: ActivityArgs):ActivityResult = {
    splog.debug(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    getSpecification.createResult(listVerifiedEmailAddresses())
  }

  def listVerifiedEmailAddresses(): List[String] = {
    sesAdapter.listVerifiedEmailAddresses()
  }
}

class EmailVerifiedAddressLister(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractEmailVerifiedAddressLister
  with LoggingWorkflowAdapterImpl
  with SESAdapterComponent {
    private lazy val _ses = new SESAdapter(_cfg)
    def sesAdapter = _ses
}

object email_verifiedaddresslister extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new EmailVerifiedAddressLister(cfg, splog)
  }
}
