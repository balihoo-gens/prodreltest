package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.config.PropertiesLoaderComponent

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: SWFAdapter
}

abstract class SWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] with PropertiesLoaderComponent {
}

