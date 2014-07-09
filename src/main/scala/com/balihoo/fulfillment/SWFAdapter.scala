package com.balihoo.fulfillment

import com.balihoo.fulfillment.config.PropertiesLoader
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.regions.Regions
import com.amazonaws.regions.Region

class SWFAdapter(loader: PropertiesLoader) {
  private val config = loader
  private val accessKey: String = loader.getString("aws.accessKey")
  private val secretKey = loader.getString("aws.secretKey")
  private val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val domain = config.getString("domain")
  val region = Region.getRegion(
    try {
      // something like "eu-west-1"
      Regions.fromName(config.getString("region")))
    } catch {
      case exception:Exception =>
        Regions.DEFAULT_REGION
      case _:Throwable =>
        throw new Exception("throwable getting region from config")
    }
  )
  val client = region.createClient(AmazonSimpleWorkflowAsyncClient, credentials, new ClientConfiguration())
}
