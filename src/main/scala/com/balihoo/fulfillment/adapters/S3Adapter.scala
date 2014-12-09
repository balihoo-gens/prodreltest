package com.balihoo.fulfillment.adapters

import java.io.{Closeable, File}
import java.net.{URI, URL}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{CannedAccessControlList, ObjectMetadata, PutObjectRequest, S3Object}
import com.amazonaws.regions.{Regions, Region}
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import resource._

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
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
 * Wrapper for an s3 object.
 * This object must be released as fast as possible per the amazon API.
 */
case class S3Meta(awsObject: S3Object, key: String, bucket: String) extends Closeable {
  lazy val lastModified = awsObject.getObjectMetadata.getLastModified
  lazy val httpsUrl = new URL("https", "s3.amazonaws.com", s"$bucket/$key")
  lazy val s3Url = new URL("s3", bucket, key)
  lazy val s3Uri = new URI(s"s3://$bucket/$key")
  lazy val userMetaData = awsObject.getObjectMetadata.getUserMetadata.asScala.toMap
  lazy val filename = awsObject.getKey.split("/").last
  override def close() = awsObject.close()
  def getContentStream = awsObject.getObjectContent
}

/**
 * Amazon Java client based s3 adapter.
 */
abstract class AbstractS3Adapter extends AWSAdapter[AmazonS3Client] {

  this: PropertiesLoaderComponent
    with SploggerComponent =>

  def defaultBucket = config.getString("s3bucket")

  implicit private def visibility2acl(visibility: S3Visibility) = visibility match {
    case PublicS3Visibility => CannedAccessControlList.PublicRead
    case PrivateS3Visibility => CannedAccessControlList.Private
  }

  /**
   * @param key s3 key i.e.: `some/long/key.file`
   * @return a `Try` to get a s3 object
   */
  def getMeta(key: String): Try[S3Meta] = getMeta(defaultBucket, key)

  /**
   * @param bucket s3 bucket
   * @param key s3 key i.e.: `some/long/key.file`
   * @return a `Try` to get a s3 object
   */
  def getMeta(bucket: String, key: String): Try[S3Meta] = Try({
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

    val metaData = new ObjectMetadata()
    metaData.setContentLength(file.length()) /* required in order to enable streaming */
    metaData.setUserMetadata(userMetaData.asJava)

    request
      .withCannedAcl(visibility)
      .withMetadata(metaData)

    val start = System.currentTimeMillis
    splog.debug(s"Uploading... key=$key bucket=$bucket file=${file.getName} visibility=$visibility")
    Try(client.putObject(request)).map { putObjectResult =>
      splog.debug(s"Uploaded! key=$key bucket=$bucket file=${file.getName} time=${System.currentTimeMillis - start}")
      new URI(s"s3://$bucket/$key")
    }
  }

  /**
   * Gets the contents of an S3 object as a string.
   * @param bucket
   * @param key
   * @param codec the character set
   * @return
   */
  def getObjectContentAsString(bucket: String, key: String)(implicit codec: Codec): String = {
    splog.debug(s"Getting object content as a string. bucket=$bucket key=$key")
    val resource = for (s3Object <- managed(getMeta(bucket, key).get);
                        inputStream <- managed(s3Object.getContentStream)) yield {
      Source.fromInputStream(inputStream).mkString
    }
    resource.acquireAndGet(s => s)
  }

}

class S3Adapter(cfg: PropertiesLoader, override val splog: Splogger)
  extends AbstractS3Adapter
    with PropertiesLoaderComponent
    with SploggerComponent {
  def config = cfg

  /**
    * S3 is 'multi-region', or, in other words, broken for any other region than east-1
    * creating a client through the region as AWSAdapter does results in a 301 error
    * from amazon. Creating a region-less instance here makes it default to east-1
    */
   protected override def createClient:AmazonS3Client = {
     new AmazonS3Client()
  }
}

object S3Adapter {
  /**
   * Breaks up an S3 URL into useful parts
   * @param url
   * @return the bucket and key
   */
  def dissectS3Url(url: URI): (String, String) = {
    require(url.getScheme != null && url.getScheme.equalsIgnoreCase("s3"), s"Invalid URL scheme in $url")

    val bucket = url.getHost
    require(bucket != null && !bucket.isEmpty, s"Missing hostname in $url")

    val path = url.getPath
    require(path != null && !path.isEmpty, s"Missing path in $url")

    // Strip leading slash from path to get the key
    val key = path.substring(1)

    (bucket, key)
  }
}
