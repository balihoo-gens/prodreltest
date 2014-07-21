package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient

class SWFAdapter(loader: PropertiesLoader) extends AWSAdapter[AmazonSimpleWorkflowAsyncClient](loader) {
}


