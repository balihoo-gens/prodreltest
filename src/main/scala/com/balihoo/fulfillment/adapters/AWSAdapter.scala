package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.config.PropertiesLoaderProvider
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentialsProvider}
import com.amazonaws.regions.{Regions, Region}
import scala.reflect.ClassTag

abstract class AWSAdapter[T <: com.amazonaws.AmazonWebServiceClient : ClassTag] {
  this: PropertiesLoaderProvider =>

  //can't have constructor code using the self type reference
  // unless it was declared 'lazy'. If not, config is still null
  // and will throw a NullPointerException at this time.
  private val accessKey: String = config.getString("aws.accessKey")
  private val secretKey = config.getString("aws.secretKey")
  private val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val domain = config.getString("domain")
  val region = Region.getRegion(
    try {
      // something like "eu-west-1"
      Regions.fromName(config.getString("region"))
    } catch {
      case exception:Exception =>
        Regions.DEFAULT_REGION
      case _:Throwable =>
        throw new Exception("throwable getting region from config")
    }
  )

  //this type cannot be resolved statically by classOf[T]
  val clientType = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[_ <: com.amazonaws.AmazonWebServiceClient]]

  val client:T = region.createClient(
    clientType,
    new AWSCredentialsProvider() {
      def getCredentials = credentials
      def refresh() {}
    },
    new ClientConfiguration()
  ).asInstanceOf[T]
}
