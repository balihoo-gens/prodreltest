package com.balihoo.fulfillment.adapters

import java.io.{FileOutputStream, File}

/**
 * Component to help accessing the local filesystem.
 * Useful mostly in tests, where you can mock it to avoid any direct FS access.
 */
trait FilesystemAdapterComponent {
  
  def filesystemAdapter: FilesystemAdapter

  trait FilesystemAdapter {

    def newTempFileOutputStream(name: String, extension: String = ".tmp"): (File, FileOutputStream)

    def newTempFile(name: String, extension: String = ".tmp"): File

    def newFileOutputStream(file: File): FileOutputStream

  }
  
}

trait LocalFilesystemAdapterComponent extends FilesystemAdapterComponent {

  val filesystemAdapter = new LocalFilesystemAdapter()

  class LocalFilesystemAdapter extends FilesystemAdapter {

    override def newTempFile(name: String, extension: String = ".tmp"): File = {
      require(!name.trim.isEmpty)
      require(!extension.trim.isEmpty)
      val file = File.createTempFile(name, extension)
      file.deleteOnExit()
      file
    }

    override def newFileOutputStream(file: File): FileOutputStream = new FileOutputStream(file)

    override def newTempFileOutputStream(name: String, extension: String = ".tmp"): (File, FileOutputStream) = {
      val file = newTempFile(name, extension)
      (file, newFileOutputStream(file))
    }

  }

}