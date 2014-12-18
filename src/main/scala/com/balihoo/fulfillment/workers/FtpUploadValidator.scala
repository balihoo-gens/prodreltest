package com.balihoo.fulfillment.workers

import java.io.IOException
import java.net.URL

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.{PropertiesLoaderComponent, FTPUploadConfig, PropertiesLoader}
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractFTPUploadValidator extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with PropertiesLoaderComponent =>

  override def getSpecification: ActivitySpecification = FTPUploadConfig.getSpecification

  override def handleTask(args: ActivityArgs):ActivityResult = {
    splog.debug(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    // Get the config data.
    val conf = new FTPUploadConfig(args, config)

    // Verify that the source file exists.
    if (conf.sourceUrl.openConnection().getContentLength < 0)
      throw new IOException("Unable to get size of " + conf.sourceUrl)

    // No exceptions, so call it good.
    getSpecification.createResult("OK")
  }
}

class FTPUploadValidator(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractFTPUploadValidator
  with LoggingWorkflowAdapterImpl
  with PropertiesLoaderComponent {
    def config = _cfg
}

object ftp_uploadvalidator extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new FTPUploadValidator(cfg, splog)
  }
}
