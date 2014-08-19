package com.balihoo.fulfillment.workers

import java.io.IOException
import java.net.URL

import com.balihoo.fulfillment.adapters.{DynamoAdapter, SWFAdapter, DynamoAdapterComponent, SWFAdapterComponent}
import com.balihoo.fulfillment.config.{PropertiesLoaderComponent, FTPUploadConfig, PropertiesLoader}

abstract class AbstractFTPUploadValidatorWorker extends FulfillmentWorker {
  this: SWFAdapterComponent
    with DynamoAdapterComponent
    with PropertiesLoaderComponent =>

  override def getSpecification: ActivitySpecification = FTPUploadConfig.getSpecification

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      // Get the config data.
      val conf = new FTPUploadConfig(params, config)

      // Verify that the source file exists.
      if (new URL(conf.sourceUrl).openConnection().getContentLength() < 0)
        throw new IOException("Unable to get size of " + conf.sourceUrl)

      // No exceptions, so call it good.
      "OK"
    }
  }
}

class FTPUploadValidatorWorker(swf: SWFAdapter, dyn: DynamoAdapter, cfg: PropertiesLoader)
  extends AbstractFTPUploadValidatorWorker
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with PropertiesLoaderComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def config = cfg
}

object ftpuploadvalidatorworker {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new FTPUploadValidatorWorker(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      cfg
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work
  }
}
