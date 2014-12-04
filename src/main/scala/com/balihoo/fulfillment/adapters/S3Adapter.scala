package com.balihoo.fulfillment.adapters

import java.io.File
import java.net.{URI, URL}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{CannedAccessControlList, ObjectMetadata, PutObjectRequest, S3Object}
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}

import scala.collection.JavaConverters._
import scala.util.Try

trait S3AdapterComponent {
  def s3Adapter: AbstractS3Adapter
}

/**
 * Visibilities of s3 objects.
 */
sealed trait S3Visibility
case object PublicS3Visibility extends S3Visibility
case object PrivateS3Visibility extends S3Visibility

/**
 * Wrapper for an s3 download as a temporary file.
 */
case class S3Download(private val tempFile: TempFile, meta: S3Meta) {
  lazy val absolutePath = tempFile.absolutePath
  def asInputStreamReader = tempFile.asInputStreamReader
  def close() = {
    meta.close()
    tempFile.delete()
  }
}

/**
 * Wrapper for an s3 object.
 */
case class S3Meta(awsObject: S3Object, key: String, bucket: String) {
  lazy val lastModified = awsObject.getObjectMetadata.getLastModified
  lazy val httpsUrl = new URL("https", "s3.amazonaws.com", s"$bucket/$key")
  lazy val s3Url = new URL("s3", bucket, key)
  lazy val s3Uri = new URI(s"s3://$bucket/$key")
  lazy val userMetaData = awsObject.getObjectMetadata.getUserMetadata.asScala.toMap
  def close() = awsObject.close()
}

/**
 * Amazon Java client based s3 adapter.
 */
abstract class AbstractS3Adapter extends AWSAdapter[AmazonS3Client] {

  this: PropertiesLoaderComponent
    with FilesystemAdapterComponent
    with SploggerComponent =>

  def defaultBucket = config.getString("s3bucket")

  def download(meta: S3Meta): S3Download = {
    splog.debug(s"Downloading... bucket=${meta.bucket} key=${meta.key}")
    val s3Stream = meta.awsObject.getObjectContent
    val tempFile = filesystemAdapter.newTempFile(s3Stream)
    splog.debug(s"Downloaded! bucket=${meta.bucket} key=${meta.key}")
    S3Download(tempFile, meta)
  }

  /**
   * @param key s3 key i.e.: `some/long/key.file`
   * @return a `Try` to get a s3 object
   */
  def get(key: String): Try[S3Meta] = get(defaultBucket, key)

  /**
   * @param bucket s3 bucket
   * @param key s3 key i.e.: `some/long/key.file`
   * @return a `Try` to get a s3 object
   */
  def get(bucket: String, key: String): Try[S3Meta] = Try({
    splog.debug(s"Getting... bucket=$bucket key=$key")
    val awsObject = client.getObject(bucket, key)
    splog.debug(s"Got it! bucket=$bucket key=$key")
    awsObject
  }).map(S3Meta(_, key, bucket))


  /**
   * Upload a local file to s3.
   *
   * @param key key name.
   * @param file local file to upload.
   * @param bucket bucket name.
   * @param userMetaData extra s3 metadata to add.
   * @param visibility object visibility in s3 (determine permissions).
   *
   * @return a s3 URI to that file.
   */
  def upload(key: String, file: File, bucket: String = defaultBucket, userMetaData: Map[String, String] = Map.empty, visibility: S3Visibility = PrivateS3Visibility): Try[URI] = {

    val request = new PutObjectRequest(bucket, key, file)

    visibility match {
      case PublicS3Visibility => request.withCannedAcl(CannedAccessControlList.PublicRead)
      case PrivateS3Visibility => request.withCannedAcl(CannedAccessControlList.Private)
    }

    val metaData = new ObjectMetadata()
    metaData.getUserMetadata.putAll(userMetaData.asJava)
    request.withMetadata(metaData)

    val start = System.currentTimeMillis
    splog.debug(s"Uploading... file=${file.getName}")
    Try(client.putObject(request)).map { putObjectResult =>
      splog.debug(s"Uploaded! file=${file.getName} time=${System.currentTimeMillis - start}")
      new URI(s"s3://$bucket/$key")
    }
  }
}

class S3Adapter(cfg: PropertiesLoader, override val splog: Splogger)
  extends AbstractS3Adapter
    with PropertiesLoaderComponent
    with SploggerComponent
    with LocalFilesystemAdapterComponent {
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
