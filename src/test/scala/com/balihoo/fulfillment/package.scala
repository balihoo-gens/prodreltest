package com.balihoo

import java.io.{FileReader, FileOutputStream, File}

import org.specs2.mutable.After
import org.specs2.specification.Scope

/**
 * Re-usable test stuff.
 */
package object fulfillment {

  trait TempFileContext extends Scope with After {
    val tempFile = File.createTempFile(getClass.getSimpleName, ".tmp")
    tempFile.deleteOnExit()
    override def after = {
      tempFile.delete()
    }
  }

  trait TempFileOutputStreamContext extends TempFileContext {
    val tempFileOutputStream = new FileOutputStream(tempFile)
    override def after = {
      tempFileOutputStream.close()
      super.after
    }
  }

  trait TempFileReaderContext extends TempFileContext {
    val tempReader = new FileReader(tempFile)
    override def after = {
      tempReader.close()
      super.after
    }
  }

}
