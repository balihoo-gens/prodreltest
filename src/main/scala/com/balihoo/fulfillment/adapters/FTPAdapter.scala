package com.balihoo.fulfillment.adapters

import java.net.URL

import com.balihoo.fulfillment.config.{FTPUploadConfig, PropertiesLoader, PropertiesLoaderComponent}
import com.balihoo.commons.client.FtpClient

trait FTPAdapterComponent {
  def ftpAdapter: AbstractFTPAdapter with PropertiesLoaderComponent
}

abstract class AbstractFTPAdapter {
  this: PropertiesLoaderComponent =>

  def uploadFile(conf: FTPUploadConfig): Unit = {
    new FtpClient(conf.ftpHost, conf.ftpPort, conf.ftpUsername, conf.ftpPassword)
      .send(new URL(conf.sourceUrl).openStream(), conf.ftpDirectory, conf.ftpFilename)
  }
}

class FTPAdapter(cfg: PropertiesLoader) extends AbstractFTPAdapter with PropertiesLoaderComponent {
  def config = cfg;
}
