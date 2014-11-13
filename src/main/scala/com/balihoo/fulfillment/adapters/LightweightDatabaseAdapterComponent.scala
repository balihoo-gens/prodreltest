package com.balihoo.fulfillment.adapters

import java.io.File
import java.sql.{PreparedStatement, DriverManager, Connection}

import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}

/**
 * Lightweight database adapter component.
 * Expose an adapter to access a self-contained, server-less, lightweight database.
 */
trait LightweightDatabaseAdapterComponent {

  type DB_TYPE <: LightweightDatabase with LightweightFileDatabase

  /**
   * @return a lightweight db instance.
   */
  def liteDbAdapter: LightweightDatabaseAdapter

  /**
   * Lightweight db adapter operations.
   */
  trait LightweightDatabaseAdapter {

    def create(name: String): DB_TYPE

  }

  /**
   * Allows to process database operation in batch.
   */
  trait DbBatch {

    /**
     * Add parameter to current batch record.
     */
    def param[T <: Any](index: Int, value: T)

    /**
     * Add current record to batch.
     */
    def add()

    /**
     * Execute current batch operations.
     */
    def execute()
  }

  /**
   * Encapsulate a db and expose operations to be done on it.
   */
  trait LightweightDatabase {

    /**
     * Execute a sql statement.
     */
    def execute(statement: String)

    /**
     * @return a new batcher to process db operations in batch.
     */
    def batch(statement: String): DbBatch

    /**
     * Execute a statement that should return a single row with a single `Int` column.
     */
    def selectCount(query: String): Int

    /**
     * Commit all statements to db.
     */
    def commit()

    /**
     * Close db.
     */
    def close()

    /**
     * @return `true` if db is closed, otherwise false.
     */
    def isClosed(): Boolean

  }

  /**
   * Lightweight database that supports file storage.
   */
  trait LightweightFileDatabase {

    /**
     * File used for database storage.
     */
    def file: File

    /**
     * Destroy db.
     */
    def destroy() = file.delete()

  }

  /**
   * A jdbc-based batch processor.
   * @param preparedStatement the jdbc `PreparedStatement`
   */
  class JdbcBatch(preparedStatement: PreparedStatement) extends DbBatch {

    override def param[T <: Any](index: Int, value: T) = value match {
      case anInt: Int => preparedStatement.setInt(index, anInt)
      case aString: String => preparedStatement.setString(index, aString)
      case aDate: java.sql.Date => preparedStatement.setDate(index, aDate)
      case aTimestamp: java.sql.Timestamp => preparedStatement.setTimestamp(index, aTimestamp)
      case anUnsupportedSqlType => throw new RuntimeException("unsupported type " + value.getClass)
    }

    override def add() = preparedStatement.addBatch()

    override def execute() = preparedStatement.executeBatch()

  }

  /**
   * A jdbc-based lite db.
   * @param connection the jdbc connection
   */
  abstract class JdbcLightweightDatabase(connection: Connection, splog: Splogger) extends LightweightDatabase {

    connection.setAutoCommit(false)

    private def newStatement() = {
      val stmt = connection.createStatement()
      stmt.setQueryTimeout(30) // 30 sec. timeout
      stmt.closeOnCompletion()
      stmt
    }

    override def batch(statement: String) = new JdbcBatch(connection.prepareStatement(statement))

    override def execute(stmt: String) = {
      connection.setAutoCommit(true)
      newStatement().executeUpdate(stmt)
      connection.setAutoCommit(false)
    }

    override def commit() = connection.commit()

    override def close() = connection.close()

    override def isClosed() = connection.isClosed()

    override def selectCount(query: String) = {
      val rs = newStatement().executeQuery(query)
      if (rs.next()) rs.getInt(1) else 0
    }

  }

}

/**
 * @inheritdoc
 * sqllite implementation
 */
trait SqlLiteLightweightDatabaseAdapterComponent extends LightweightDatabaseAdapterComponent {

  this: SploggerComponent =>

  override type DB_TYPE = SqlLiteLightweightDatabase

  override val liteDbAdapter = new SqlLiteLightweightDatabaseAdapter

  class SqlLiteLightweightDatabaseAdapter extends LightweightDatabaseAdapter {

    /* Load driver, make sure it exists */
    Class.forName("org.sqlite.JDBC")

    override def create(name: String): SqlLiteLightweightDatabase = {
      val file = File.createTempFile(name, ".tmp")
      val path = file.getAbsolutePath
      val connection = DriverManager.getConnection(s"jdbc:sqlite:$path")
      val db = new SqlLiteLightweightDatabase(file, connection)
      db.optimize()
      db
    }

  }

  class SqlLiteLightweightDatabase(override val file: File, connection: Connection)
    extends JdbcLightweightDatabase(connection, splog)
      with LightweightFileDatabase {

    def optimize() = {
      execute("PRAGMA synchronous = OFF")
      execute("PRAGMA journal_mode = OFF")
      execute("PRAGMA locking_mode = EXCLUSIVE")
      execute("PRAGMA temp_store = MEMORY")
      execute("PRAGMA count_changes = OFF")
      execute("PRAGMA temp_store = MEMORY")
    }

  }

}