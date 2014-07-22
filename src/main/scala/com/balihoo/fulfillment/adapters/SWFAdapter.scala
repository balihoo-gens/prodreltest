package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.config.PropertiesLoaderProvider

//for the cake pattern dependency injection
trait SWFAdapterProvider {
  val swfAdapter: SWFAdapter
  abstract class SWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] with PropertiesLoaderProvider {
  }
}


