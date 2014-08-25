package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.{PropertiesLoaderComponent, FTPUploadConfig, PropertiesLoader}

abstract class AbstractFTPUploader extends FulfillmentWorker {
  this: FTPAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent
    with PropertiesLoaderComponent =>

  override def getSpecification: ActivitySpecification = FTPUploadConfig.getSpecification

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      ftpAdapter.uploadFile(new FTPUploadConfig(params, config))

      // No exceptions, so call it good.
      "OK"
    }
  }
}

class FTPUploader(swf: SWFAdapter, dyn: DynamoAdapter, ftp: FTPAdapter, cfg: PropertiesLoader)
  extends AbstractFTPUploader
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with FTPAdapterComponent
  with PropertiesLoaderComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def ftpAdapter = ftp
    def config = cfg
}

object ftp_uploader {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new FTPUploader(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new FTPAdapter(cfg),
      cfg
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
