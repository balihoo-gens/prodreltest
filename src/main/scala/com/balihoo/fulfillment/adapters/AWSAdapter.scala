package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config.PropertiesLoaderComponent
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentialsProvider}
import com.amazonaws.regions.{Regions, Region}
import scala.reflect.{ClassTag, classTag}
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.AmazonWebServiceClient

abstract class AWSAdapter[T <: AmazonWebServiceClient : ClassTag] {
  this: PropertiesLoaderComponent =>

  //can't have constructor code using the self type reference
  // unless it was declared 'lazy'. If not, config is still null
  // and will throw a NullPointerException at this time.
  lazy val region:Region = getRegion
  lazy val client:T = createClient

  //put this all in a method rather than just in the constructor to
  // make it easier to Mock this
  protected def createClient:T = {

    //this type cannot be resolved statically by classOf[T]
    val clientType = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[_ <: AmazonWebServiceClient]]

    val awsClientConfig = {
      val SWFTag = classTag[AmazonSimpleWorkflowAsyncClient]
      classTag[T] match {
        case SWFTag =>
          val swfSocketTimeout = config.getOrElse("swfSocketTimeoutMs", 70*1000)
          new ClientConfiguration().withSocketTimeout(swfSocketTimeout)
        case _ => null
      }
    }

    //No credentials are provided. This means that the client will first look in the environment
    //then system properties and finally the IMDS for IAM roles.
    //If no IAM role is available (e.g. running local), creds should be in the env
    region.createClient(
      clientType,
      null,
      awsClientConfig
    ).asInstanceOf[T]
  }

  private def getRegion:Region = {
    Region.getRegion(
      try {
        // something like "eu-west-1"
        Regions.fromName(config.getString("region"))
      } catch {
        case e:Exception =>
          val dr = Regions.DEFAULT_REGION
          println(s"Unable to get region: ${e.getMessage}\n  region defaulting to ${dr.getName}")
          dr
      }
    )
  }
}
