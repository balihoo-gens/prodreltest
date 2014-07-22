package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient

//for the cake pattern dependency injection
trait SWFAdapterProvider {
  val swfAdapter: SWFAdapter
}

class SWFAdapter(loader: PropertiesLoader) extends AWSAdapter[AmazonSimpleWorkflowAsyncClient](loader) {
}

