package com.balihoo.fulfillment.workers

import java.io.File

import com.sun.xml.internal.ws.api.model.ExceptionType

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
    import scala.util.control.Exception.ignoring
    for (resource <- resources) {
      ignoring(classOf[Exception]) {
        resource.close()
      }
    }
    resources.clear()
    for (file <- fileResources) {
      ignoring(classOf[Exception]) {
        file.delete()
      }
    }
    fileResources.clear()
  }

}
