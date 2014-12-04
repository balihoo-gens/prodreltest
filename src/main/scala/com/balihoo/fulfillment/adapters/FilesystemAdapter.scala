package com.balihoo.fulfillment.adapters

import java.io._

import com.balihoo.fulfillment.util.SploggerComponent
import org.apache.commons.io.IOUtils

/**
 * Component to help accessing the local filesystem.
 * Useful mostly in tests, where you can mock it to avoid any direct FS access.
 */
trait FilesystemAdapterComponent {
  
  def filesystemAdapter: FilesystemAdapter

  trait FilesystemAdapter {

    def newTempFile(in: InputStream): TempFile

    def newTempFile(): TempFile

  }
  
}

case class TempFile(file: File) {

  def delete() = file.delete()

  lazy val absolutePath = file.getAbsolutePath
  lazy val uri = file.toURI
  def asInputStream: InputStream = new FileInputStream(file)
  def asInputStreamReader: InputStreamReader = new InputStreamReader(asInputStream)
  def asOutputStream: OutputStream = new FileOutputStream(file)
}

trait LocalFilesystemAdapterComponent extends FilesystemAdapterComponent {

  this: SploggerComponent =>

  val filesystemAdapter = new LocalFilesystemAdapter()

  class LocalFilesystemAdapter extends FilesystemAdapter {

    def newTempFile() = TempFile(newFile())

    /**
     * Create a new temporary file that contains specified input stream content.
     * @param in a stream to copy data from.
     * @return a new temp file handle.
     */
    override def newTempFile(in: InputStream) = {
      val file = newFile()
      val out = new FileOutputStream(file)
      try {
        val copied = IOUtils.copy(in, out)
        if (copied == 0) throw new IllegalArgumentException("No data copied")
        splog.debug(s"Temporary file data copied bytesCount=$copied")
      } finally {
        out.close()
      }
      TempFile(file)
    }

    /**
     * @return a new temp file handle.
     */
    private def newFile() = {
      val file = File.createTempFile(getClass.getSimpleName, ".tmp")
      splog.debug("New temporary file path=" + file.getAbsolutePath)
      file.deleteOnExit()
      file
    }

  }

}