package com.balihoo.fulfillment

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.deciders._

object main {
  def main(args: Array[String]) {
    val progs = Map(
      "adwords_campaignprocessor" -> adwords_campaignprocessor.main _,
      "listverifiedemailaddressworker" -> listverifiedemailaddressworker.main _,
      "chaosworker" -> chaosworker.main _,
      "adwords_campaignprocessor" -> adwords_campaignprocessor.main _,
      "verifyemailaddressworker" -> verifyemailaddressworker.main _,
      "timezoneworker" -> timezoneworker.main _,
      "adwords_accountlookup" -> adwords_accountlookup.main _,
      "adwords_accountcreator" -> adwords_accountcreator.main _,
      "adwords_adgroupprocessor" -> adwords_adgroupprocessor.main _,
      "coordinator" -> coordinator.main _,
      "sendemailworker" -> sendemailworker.main _,
      "adwords_imageadprocesso" -> adwords_imageadprocessor.main _
    )
    val progName = args.headOption
    progName match {
      case Some(name) =>
        progs(name)(args.tail)
      case None =>
        println("please provide program name and arguments:")
        progs.keys.foreach(println)
    }
  }
}

