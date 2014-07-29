package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.config._

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: SWFAdapter with PropertiesLoaderComponent
}

abstract class SWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] {
  this: PropertiesLoaderComponent =>
}

object SWFAdapter {
  def apply(cfg: PropertiesLoader) = {
    new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
  }
}
