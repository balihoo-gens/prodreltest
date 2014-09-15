package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config._
import com.amazonaws.services.sqs.AmazonSQSClient

//for the cake pattern dependency injection
trait SQSAdapterComponent {
  def sqsAdapter: AbstractSQSAdapter with PropertiesLoaderComponent
}

abstract class AbstractSQSAdapter extends AWSAdapter[AmazonSQSClient] {
  this: PropertiesLoaderComponent =>
}

class SQSAdapter(cfg: PropertiesLoader) extends AbstractSQSAdapter with PropertiesLoaderComponent {
  def config = cfg
}
