package com.balihoo.fulfillment.workers

import java.io.File

import scala.collection.mutable.ListBuffer

trait WithResources {

  private var resources = ListBuffer[AutoCloseable]()
  private var fileResources = ListBuffer[File]()

  def workerResource[T <: AutoCloseable](resource: T): T = {
    resources += resource
    resource
  }

  def workerFile[T <: File](file: T): T = {
    fileResources += file
    file
  }

  def closeResources() = {
    for (resource <- resources) {
      resource.close()
    }
    for (file <- fileResources) {
      file.delete()
    }
  }

}
