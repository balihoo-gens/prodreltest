package com.balihoo.fulfillment.adapters

import java.io._
import java.net.URI
import java.nio.charset.Charset
import java.nio.file._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{S3Object, CannedAccessControlList, PutObjectRequest}
import com.amazonaws.auth.BasicAWSCredentials
import com.balihoo.fulfillment.config._
import resource._

import scala.io.{Codec, Source}

//for the cake pattern dependency injection
trait S3AdapterComponent {
  def s3Adapter: AbstractS3Adapter
}

abstract class AbstractS3Adapter extends AWSAdapter[AmazonS3Client] {
  this: PropertiesLoaderComponent =>

  val tmpDir = System.getProperty("java.io.tmpdir")

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

  def getAsTempFile(bucket: String, key: String, filePrefix: Option[String] = None) = {
    withS3Object(bucket, key) { s3obj =>
      val prefix = filePrefix.getOrElse(getClass.getSimpleName)
      // TODO (jmelanson) inject and use FilesystemComponent
      val tmpFile = File.createTempFile(prefix, ".db")
      val bytesCount = fileCopy(s3obj.getObjectContent, Paths.get(tmpFile.getPath))
      if (bytesCount == 0.toLong) throw new RuntimeException("No data copied from s3 object.")
      tmpFile
    }
  }

  protected def fileCopy(is: InputStream, path: Path) = Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING)

  /**
   * Warning: don't forget to close your reader!
   * @return a new `InputStreamReader` from the `S3ObjectInputStream` with `utf-8` as default encoding.
   */
  def getObjectContentAsReader(bucket: String, key: String, charsetName: String = "UTF-8") = {
    withS3Object(bucket, key) { s3obj =>
      new InputStreamReader(s3obj.getObjectContent, Charset.forName(charsetName: String))
    }
  }

  def getObjectContentAsString(bucket: String, key: String)(implicit codec: Codec): String = {
    managed(withS3Object(bucket, key) { s3obj => s3obj.getObjectContent })
      .map(inputStream => Source.fromInputStream(inputStream).mkString).opt.get
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
