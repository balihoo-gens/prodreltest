package com.balihoo.fulfillment.adapters

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.io.IOUtils

/**
 * The idea of this component is to allow simpler unit testing for
 * workers that uses the filesystem. It makes all operations faster
 * and mockable.
 */

/**
 * Component that offer a filesystem adapter.
 */
trait FilesystemAdapterComponent {

  /**
   * @return a file system adapter.
   */
  val filesystemAdapter = new JavaIOFilesystemAdapter

}

/**
 * Exception thrown when a copy operation fails.
 */
case object NoDataCopiedException extends IOException("No data copied")

/**
 * A filesystem adapter implementation based on simple Java IO classes.
 */
class JavaIOFilesystemAdapter {

  /**
   * @return a new temporary file with the content of the specified input
   *         stream with a name based on the specified hint.
   */
  def newTempFileFromStream(in: InputStream, hint: String): File = {
    val file = newTempFile(hint)
    val out = newOutputStream(file)
    try {
      if (IOUtils.copy(in, out) == 0)
        throw NoDataCopiedException
      else
        file
    } finally {
      out.close()
    }
  }

  /**
   * @return a new temporary file with a name based on the specified hint.
   */
  def newTempFile(hint: String): File = {
    val safeHint = hint.replaceAll("/", "_")
    val file = File.createTempFile(safeHint, ".tmp")
    file.deleteOnExit()
    file
  }

  /**
   * @return a new output stream from the specified file.
   *         Caller need to close that stream.
   */
  def newOutputStream(file: File): OutputStream = new FileOutputStream(file)

  /**
   * @return a new input stream from the specified file.
   *         Caller need to close that stream.
   */
  def newInputStream(file: File): InputStream = new  FileInputStream(file)

  /**
   * @return a new reader for the specified input stream.
   */
  def newReader(in: InputStream): Reader = new InputStreamReader(in)

  /**
   * @return a new GZIP input stream, to decode incoming compressed data.
   */
  def newGZIPInputStream(in: InputStream) = new GZIPInputStream(in)

  /**
   * @return a new GZIP output stream, to encode outcoming compressed data.
   */
  def newGZIPOutputStream(out: OutputStream) = new GZIPOutputStream(out)

  /**
   * Compress a file in the same directory as the original file.
   * @return the compressed file handle.
   */
  def gzip(file: File, extension: String = ".gz"): File = {
    import resource._
    val gzFile = new File(file.getParentFile, file.getName + extension)
    (for {
      in <- managed(newInputStream(file))
      out <- managed(new GZIPOutputStream(new FileOutputStream(gzFile)))
    } yield {
      if (IOUtils.copy(in, out) > 0)
        gzFile
      else
        throw NoDataCopiedException
    }).acquireAndGet(f => f)
  }

}

