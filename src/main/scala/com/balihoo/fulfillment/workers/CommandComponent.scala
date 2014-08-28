package com.balihoo.fulfillment.workers

import java.io.InputStream
import scala.io.Source
import scala.sys.process._

trait CommandComponent {
  def command: Command

  class CommandResult(val code:Int, val out:String, val err:String)

  class Command(commandLine:String) {

    def streamToString(stream:InputStream):String = {
      Source.fromInputStream(stream).mkString("")
    }

    def run(input:String):CommandResult = {
      var out = ""
      var err = ""
      val process = (commandLine:ProcessBuilder).run(
        new ProcessIO(
          stdin  => { stdin.write(input.getBytes)
            stdin.close() },
          stdout => { out = streamToString(stdout)
            stdout.close() },
          stderr => { err = streamToString(stderr)
            stderr.close() })
      )
      new CommandResult(process.exitValue(), out, err)
    }
  }
}

