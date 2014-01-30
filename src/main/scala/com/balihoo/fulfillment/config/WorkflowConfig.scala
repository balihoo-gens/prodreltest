package com.balihoo.fulfillment.config

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.workers.PrototypeListProviderWorkerConfig
import com.balihoo.fulfillment.PrototypeWorkflowExecutorConfig

object WorkflowConfig {

  //might want to allow this filename to be passed in later.
  //would need to make sure every main passes the same file
  val properties: PropertiesLoader = new PropertiesLoader(".fulfillment.properties")

  //*** Global configs pulled from properties file ***/
  val domain: String = properties.getString("domain")
  private val accessKey: String = properties.getString("aws.accessKey")
  private val secretKey = properties.getString("aws.secretKey")
  private val creds = new BasicAWSCredentials(accessKey, secretKey)
  val client = new AmazonSimpleWorkflowAsyncClient(creds)//async client has sync methods too
}

object WorkflowRegister {
  def main(args: Array[String]) {
    PrototypeWorkflowExecutorConfig.registerWorkflowType()
    PrototypeListProviderWorkerConfig.registerActivityType()
  }
}