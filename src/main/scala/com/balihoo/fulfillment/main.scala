package com.balihoo.fulfillment

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.deciders._

object main {
  def main(args: Array[String]) {
    Map(
      "sendemail" -> listverifiedemailaddressworker,
    ).main(args.tail)
  }
}

