package com.balihoo.fulfillment.adapters.test

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._


object SWFAdapter {
  def main(args: Array[String]) {
    val test = new TestSWFDomainRegistry(new SWFAdapter(PropertiesLoader(args, "aws")))
    test.run()
  }

  class TestSWFDomainRegistry(sa: SWFAdapter) extends SWFAdapterComponent {
    def swfAdapter = sa
    def run() = {
    }
  }
}


