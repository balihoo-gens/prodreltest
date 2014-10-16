package com.balihoo.fulfillment.adapters.test

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._

object SWFAdapter {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    val cfg = PropertiesLoader(args, name)

    object test extends SWFAdapterComponent {
      def swfAdapter = new SWFAdapter(cfg, splog)
      def run() = { }
    }

    test.run()
  }
}


