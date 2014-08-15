package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.{PropertiesLoaderComponent, FTPUploadConfig, PropertiesLoader}

abstract class AbstractFTPUploadWorker extends FulfillmentWorker {
  this: FTPAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent
    with PropertiesLoaderComponent =>

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      ftpAdapter.uploadFile(new FTPUploadConfig(params, config))
    }
  }
}

class FTPUploadWorker(swf: SWFAdapter, dyn: DynamoAdapter, ftp: FTPAdapter, cfg: PropertiesLoader)
  extends AbstractFTPUploadWorker
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with FTPAdapterComponent
  with PropertiesLoaderComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def ftpAdapter = ftp
    def config = cfg
}

object ftpuploadworker {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new FTPUploadWorker(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new FTPAdapter(cfg),
      cfg
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
