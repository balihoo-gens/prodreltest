package com.balihoo.fulfillment.adapters

import com.amazonaws.services.s3.AmazonS3AsyncClient
import com.balihoo.fulfillment.config._

//for the cake pattern dependency injection
trait S3AdapterComponent {
  def s3Adapter: AbstractS3Adapter with PropertiesLoaderComponent
}

abstract class AbstractS3Adapter extends AWSAdapter[AmazonS3AsyncClient] {
  this: PropertiesLoaderComponent =>
}

class S3Adapter(cfg: PropertiesLoader) extends AbstractS3Adapter with PropertiesLoaderComponent {
  def config = cfg
}
