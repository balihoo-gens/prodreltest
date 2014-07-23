package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config.PropertiesLoaderComponent
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.sqs.AmazonSQSClient

//for the cake pattern dependency injection
trait SQSAdapterComponent {
  val sqsAdapter: SQSAdapter
}

abstract class SQSAdapter extends AWSAdapter[AmazonSQSClient] with PropertiesLoaderComponent {
}
