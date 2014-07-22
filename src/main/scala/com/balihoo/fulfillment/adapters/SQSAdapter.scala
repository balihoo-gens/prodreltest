package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config.PropertiesLoaderProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.sqs.AmazonSQSClient

//for the cake pattern dependency injection
trait SQSAdapterProvider {
  val sqsAdapter: SQSAdapter
}

abstract class SQSAdapter extends AWSAdapter[AmazonSQSClient] with PropertiesLoaderProvider {
}
