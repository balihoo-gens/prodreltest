package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.{PropertiesLoaderComponent, FTPUploadConfig, PropertiesLoader}
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractFTPUploader extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with FTPAdapterComponent
  with PropertiesLoaderComponent =>

  override def getSpecification: ActivitySpecification = FTPUploadConfig.getSpecification

  override def handleTask(params: ActivityArgs):ActivityResult = {
    splog.debug(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    ftpAdapter.uploadFile(new FTPUploadConfig(params, config))

      // No exceptions, so call it good.
    getSpecification.createResult("OK")
  }
}

class FTPUploader(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractFTPUploader
  with LoggingWorkflowAdapterImpl
  with FTPAdapterComponent
  with PropertiesLoaderComponent {
    def config = _cfg

    lazy private val _ftp = new FTPAdapter(config)
    def ftpAdapter = _ftp
}

object ftp_uploader extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new FTPUploader(cfg, splog)
  }
}
