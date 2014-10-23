package com.balihoo.fulfillment

import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object UTCFormatter {

  val SEC_IN_MS = 1000
  val MIN_IN_MS = SEC_IN_MS * 60
  val HOUR_IN_MS = MIN_IN_MS * 60
  val DAY_IN_MS = HOUR_IN_MS * 24

  val dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC()

  def format(dateTime:DateTime): String = {
    dateTimeFormatter.print(dateTime)
  }
  def format(date:Date): String = {
    dateTimeFormatter.print(new DateTime(date))
  }


}
