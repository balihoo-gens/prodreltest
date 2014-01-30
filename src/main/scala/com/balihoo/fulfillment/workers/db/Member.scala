package com.balihoo.fulfillment.workers.db

import scala.slick.driver.SQLiteDriver.simple._
import org.joda.time.DateTime
import Database.threadLocalSession //implicitly used, don't remove
import slick.lifted.MappedTypeMapper

/**
 * The database has several more columns listed below but commented out of the case class.
 * The reason they are not used it threefold
 * 1) Domain driven design dictates that you only include the stuff you need, not everything in case you ever need it.
 * 2) case classes are limited to 22 values.  It is possible to make a regular class and implement the functions
 *    that case classes provide manually, but...
 * 3) projections of tuples are also limited to 22 values, so the table's * simple projection doesn't work.
 * There may be workarounds for 2 and 3, but ultimately because of 1 I won't bother.
 * Note that they still exist in the Table object and can be used for filters,
 * but they won't be projected onto the return object type.
 */
case class Member(
  affiliatenumber: Int,
  brandkey: String,
  firstname: String,
  lastname: String,
  birthdate: DateTime,
//  age: Int,
  gender: String,
//  income: Int,
  email: String
//  emailsubscribed: DateTime,
//  emailunsubscribed: DateTime,
//  emailbounced: DateTime,
//  address1: String,
//  city: String,
//  stateprovince: String,
//  postalcode: String,
//  movedin: DateTime,
//  phone: String,
//  mobile: String,
//  year: Int,
//  make: String,
//  model: String,
//  lastvisit: DateTime,
//  lastspendamount: Int,
//  mileage: Int,
//  loyaltyprogram: String,
//  ecareclub: Int,
//  householdmembers: Int,
//  haschildren: Int,
//  catowner: Int,
//  dogowner: Int,
//  petowner: Int,
//  address2: String,
//  country: String
)


object Members extends Table[Member]("recipient") {

  def id = column[Int]("recipientid", O.PrimaryKey, O.NotNull)
  def affiliatenumber = column[Int]("affiliatenumber", O.NotNull)
  def brandkey = column[String]("brandkey", O.NotNull, O.DBType("VARCHAR(10)"))//todo: does DBType limit writes?
  def firstname = column[String]("firstname", O.NotNull, O.DBType("VARCHAR(32)"))
  def lastname = column[String]("lastname", O.NotNull, O.DBType("VARCHAR(32)"))
  def birthdate = column[DateTime]("birthdate", O.NotNull)
  def age = column[Int]("age", O.NotNull)
  def gender = column[String]("gender", O.NotNull, O.DBType("CHAR(1)"))//note: no default mapper from Char to Char(1)
  def income = column[Int]("income", O.NotNull)
  def email = column[String]("email", O.NotNull, O.DBType("VARCHAR(70)"))
  def emailsubscribed = column[DateTime]("emailsubscribed")
  def emailunsubscribed = column[DateTime]("emailunsubscribed")
  def emailbounced = column[DateTime]("emailbounced")
  def address1 = column[String]("address1", O.NotNull, O.DBType("VARCHAR(50)"))
  def city = column[String]("city", O.NotNull, O.DBType("VARCHAR(30)"))
  def stateprovince = column[String]("stateprovince", O.NotNull, O.DBType("CHAR(2)"))
  def postalcode = column[String]("postalcode", O.NotNull, O.DBType("VARCHAR(5)"))
  def movedin = column[DateTime]("movedin")
  def phone = column[String]("phone", O.NotNull, O.DBType("CHAR(12)"))
  def mobile = column[String]("mobile", O.NotNull, O.DBType("CHAR(12)"))
  def year = column[Int]("year", O.Nullable)//todo: same as column def NULL?
  def make = column[String]("make", O.NotNull, O.DBType("VARCHAR(25)"))
  def model = column[String]("model", O.NotNull, O.DBType("VARCHAR(30)"))
  def lastvisit = column[DateTime]("lastvisit", O.NotNull)
  def lastspendamount = column[Int]("lastspendamount", O.NotNull)
  def mileage = column[Int]("mileage", O.NotNull)
  def loyaltyprogram = column[String]("loyaltyprogram", O.NotNull, O.DBType("VARCHAR(20)"))
  def ecareclub = column[Int]("ecareclub", O.NotNull)
  def householdmembers = column[Int]("householdmembers", O.NotNull)
  def haschildren = column[Int]("haschildren", O.NotNull)
  def catowner = column[Int]("catowner", O.NotNull)
  def dogowner = column[Int]("dogowner", O.NotNull)
  def petowner = column[Int]("petowner", O.NotNull)
  def address2 = column[String]("address2", O.Nullable, O.DBType("VARCHAR(20)"))
  def country = column[String]("country", O.Nullable, O.DBType("VARCHAR(20)"))

  //default sql.Date maps as a timestamp long, which doesn't compare with sqlite dates
  //So need to use a type mapper, as as long as I'm doing that might as well use the better joda DateTime.
  implicit val dateTimeMapping = MappedTypeMapper.base[DateTime, String](
    {dt =>
      if (dt == null) {
        null
      } else {
        dt.toString("yyyy-DD-yy")
      }
    },
    {s =>
      if (s == "" || s == null) {
        null
      } else {
        DateTime.parse(s)
      }
    }
  )

  def * = id ~ brandkey ~ firstname ~ lastname ~ birthdate ~ gender ~ email <> (Member, Member.unapply _)


  //test query. This type of logic should go in the place doing the querying, not in the table object.
  def query(birthDateDaysOffsetMin: Int, birthDateDaysOffsetMax: Int): List[Member] = {
    val now: DateTime = org.joda.time.DateTime.now()
    val jodaMin: DateTime = now.plusDays(birthDateDaysOffsetMin)
    val jodaMax: DateTime = now.plusDays(birthDateDaysOffsetMax)

    val q = Query(Members)
      .where(_.birthdate.between(jodaMin, jodaMax))

    println(q.selectStatement)

    val DB = Database.forURL("jdbc:sqlite:sample.db", driver="org.sqlite.JDBC")
    DB.withSession { implicit s: Session =>
      val list: List[Member] = q.list()(s)//for some reason doesn't find the implicit. Can pass explicitly
      println("done getting list")
      list
    }
  }
}


