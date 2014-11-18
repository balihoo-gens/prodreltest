package com.balihoo.fulfillment.adapters

import java.io.{InputStreamReader, File}
import java.nio.charset.Charset
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{S3Object, CannedAccessControlList, PutObjectRequest}
import com.amazonaws.auth.BasicAWSCredentials
import com.balihoo.fulfillment.config._

//for the cake pattern dependency injection
trait S3AdapterComponent {
  def s3Adapter: AbstractS3Adapter
}

abstract class AbstractS3Adapter extends AWSAdapter[AmazonS3Client] {
  this: PropertiesLoaderComponent =>

  def putPublic(bucket: String, key: String, file: File) = {
    client.putObject(
      new PutObjectRequest(bucket, key, file)
        .withCannedAcl(CannedAccessControlList.PublicRead)
    )
  }

  def withS3Object[T](bucket: String, key: String)(callback: (S3Object) => T) = {
    val s3obj = client.getObject(bucket, key)
    callback(s3obj)
  }

  /**
   * Warning: don't forget to close your reader!
   * @return a new `InputStreamReader` from the `S3ObjectInputStream` with `utf-8` as default encoding.
   */
  def getObjectContentAsReader(bucket: String, key: String, charsetName: String = "UTF-8") = {
    withS3Object(bucket, key) { s3obj =>
      new InputStreamReader(s3obj.getObjectContent, Charset.forName(charsetName: String))
    }
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
