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

object Abbreviator {
  /**
    * abbreviate a string with dots
    * @param s the string to abbreviate
    * @param n the maximum number of characters of the resulting string
    * @returns the abbreviated or original string
    */
  def ellipsis(s: String, n: Int) = {
    if (s.size > n && n > 3) {
      s"${s.take(n-3)}..."
    } else {
      s.take(n)
    }
  }
}
