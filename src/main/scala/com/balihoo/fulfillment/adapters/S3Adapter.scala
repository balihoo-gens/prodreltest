package com.balihoo.fulfillment.adapters

import java.io.{Reader, InputStreamReader, FileOutputStream, File}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{GetObjectRequest, CannedAccessControlList, PutObjectRequest}
import com.amazonaws.auth.BasicAWSCredentials
import com.balihoo.fulfillment.config._
import org.apache.commons.io.IOUtils

//for the cake pattern dependency injection
trait S3AdapterComponent {
  def s3Adapter: AbstractS3Adapter
}

abstract class AbstractS3Adapter extends AWSAdapter[AmazonS3Client] {
  this: PropertiesLoaderComponent =>

  def putPublic(bucket:String, key:String, file:File) = {
      client.putObject(
        new PutObjectRequest(bucket, key, file)
          .withCannedAcl(CannedAccessControlList.PublicRead)
      )
  }

  def getObjectContentAsStreamReader(bucket: String, key: String): Reader = {
    val s3object = client.getObject(bucket: String, key: String)
    new InputStreamReader(s3object.getObjectContent)
  }
}

class S3Adapter(cfg: PropertiesLoader) extends AbstractS3Adapter with PropertiesLoaderComponent {
  def config = cfg

  /**
    * S3 is 'multi-region', or, in other words, broken for any other region than east-1
    * creating a client through the region as AWSAdapter does results in a 301 error
    * from amazon. Creating a region-less instance here makes it default to east-1
    */
  protected override def createClient:AmazonS3Client = {
    val accessKey: String = config.getString("aws.accessKey")
    val secretKey = config.getString("aws.secretKey")
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    new AmazonS3Client(credentials)
  }
}
