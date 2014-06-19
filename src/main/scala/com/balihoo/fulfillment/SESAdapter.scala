package com.balihoo.fulfillment

import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceAsyncClient

class SESAdapter(loader: PropertiesLoader) {
  private val accessKey: String = loader.getString("aws.accessKey")
  private val secretKey = loader.getString("aws.secretKey")

  private val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val client = new AmazonSimpleEmailServiceAsyncClient(credentials)

  val config = loader

}
