package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.config._

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: AbstractSWFAdapter with PropertiesLoaderComponent
}

abstract class AbstractSWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] {
  this: PropertiesLoaderComponent =>
}

class SWFAdapter(cfg: PropertiesLoader) extends AbstractSWFAdapter with PropertiesLoaderComponent {
  def config = cfg
}
