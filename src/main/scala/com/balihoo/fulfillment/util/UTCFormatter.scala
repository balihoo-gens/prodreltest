package com.balihoo.fulfillment.util

import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object UTCFormatter {

  private val dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC()

  def format(dateTime:DateTime): String = {
    dateTimeFormatter.print(dateTime)
  }
  def format(date:Date): String = {
    dateTimeFormatter.print(new DateTime(date))
  }

}
