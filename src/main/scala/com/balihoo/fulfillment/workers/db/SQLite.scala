package com.balihoo.fulfillment.workers.db

import scala.slick.session.Database
import scala.slick.jdbc.StaticQuery
import scala.slick.jdbc.meta.MTable

//https://github.com/daithiocrualaoich/scala-sqlite-example
class SQLite(name: String) {
  import Database.threadLocalSession

  val database = Database.forURL(
    "jdbc:sqlite:%s.db" format name,
    driver = "org.sqlite.JDBC")

  implicit class DatabaseOps(database: Database) {
    def apply(sql: String) {
      database withSession {
        StaticQuery.updateNA(sql).execute
      }
    }

    def tableNames(): Set[String] = database withSession {
      (MTable.getTables.list() map { _.name.name }).toSet
    }
  }
}
