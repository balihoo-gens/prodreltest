package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.sqs.AmazonSQSClient

class SQSAdapter(loader: PropertiesLoader) {
  private val accessKey: String = loader.getString("aws.accessKey")
  private val secretKey = loader.getString("aws.secretKey")

  private val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val client = new AmazonSQSClient(credentials)

  val config = loader

}
