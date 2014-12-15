package com.balihoo.fulfillment.config

import java.net.URI

import com.balihoo.fulfillment.workers._

trait FTPUploadConfigComponent {
  def ftpUploadConfig: AbstractFTPUploadConfig with PropertiesLoaderComponent
}

abstract class AbstractFTPUploadConfig(params: ActivityArgs) {
  this: PropertiesLoaderComponent =>

  val sourceUrl = params[URI]("sourceUrl").toURL
  val ftpHost = params[String]("ftpHost")
  val ftpPort = params.getOrElse[Int]("ftpPort", 21)
  val ftpUsername = params[String]("ftpUsername")
  val ftpPassword = params[String]("FtpPassword")
  val ftpDirectory = params.getOrElse("ftpDirectory", "/")
  val ftpFilename = params[String]("ftpFilename")
}

class FTPUploadConfig(params: ActivityArgs, cfg: PropertiesLoader)
  extends AbstractFTPUploadConfig(params)
  with PropertiesLoaderComponent {

  def config = cfg
}

object FTPUploadConfig {
  def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new UriParameter("sourceUrl", "The URL of the file to be uploaded"),
      new HostnameParameter("ftpHost", "The destination host name"),
      new IntegerParameter("ftpPort", "The destination port number (default = 21)", required = false),
      new StringParameter("ftpUsername", "The username for the destination FTP server"),
      new EncryptedParameter("ftpPassword", "The password for the destination FTP server"),
      new StringParameter("ftpDirectory", "The destination directory (default = \"/\")", required = false),
      new StringParameter("ftpFilename", "The destination filename")
    ), new StringResultType("A status message"))
  }
}
