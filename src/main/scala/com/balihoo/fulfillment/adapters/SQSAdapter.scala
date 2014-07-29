package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config._
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.sqs.AmazonSQSClient

//for the cake pattern dependency injection
trait SQSAdapterComponent {
  def sqsAdapter: SQSAdapter with PropertiesLoaderComponent
}

abstract class SQSAdapter extends AWSAdapter[AmazonSQSClient] {
  this: PropertiesLoaderComponent =>
}

object SQSAdapter {
  def apply(cfg: PropertiesLoader) = {
    new SQSAdapter with PropertiesLoaderComponent { def config = cfg }
  }
}
