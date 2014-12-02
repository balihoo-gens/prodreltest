package com.balihoo.fulfillment.config

import java.net.URI

import com.balihoo.fulfillment.workers._

trait FTPUploadConfigComponent {
  def ftpUploadConfig: AbstractFTPUploadConfig with PropertiesLoaderComponent
}

abstract class AbstractFTPUploadConfig(params: ActivityParameters) {
  this: PropertiesLoaderComponent =>

  val sourceUrl = params[URI]("sourceUrl").toURL
  val ftpHost = params("ftpHost")
  val ftpPort = params.getOrElse[Int]("ftpPort", 21)
  val ftpUsername = params("ftpUsername")
  val ftpPassword = params("FtpPassword")
  val ftpDirectory = params.getOrElse("ftpDirectory", "/")
  val ftpFilename = params("ftpFilename")
}

class FTPUploadConfig(params: ActivityParameters, cfg: PropertiesLoader)
  extends AbstractFTPUploadConfig(params)
  with PropertiesLoaderComponent {

  def config = cfg
}

object FTPUploadConfig {
  def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new UriActivityParameter("sourceUrl", "The URL of the file to be uploaded"),
      new HostnameActivityParameter("ftpHost", "The destination host name"),
      new IntegerActivityParameter("ftpPort", "The destination port number (default = 21)", required = false),
      new StringActivityParameter("ftpUsername", "The username for the destination FTP server"),
      new EncryptedActivityParameter("ftpPassword", "The password for the destination FTP server"),
      new StringActivityParameter("ftpDirectory", "The destination directory (default = \"/\")", required = false),
      new StringActivityParameter("ftpFilename", "The destination filename")
    ), new StringActivityResult("A status message"))
  }
}
