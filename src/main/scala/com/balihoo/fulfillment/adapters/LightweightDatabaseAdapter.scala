package com.balihoo.fulfillment.adapters

import java.sql._

import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}

/**
 * Lightweight database adapter component.
 * Expose an adapter to access a self-contained, server-less, lightweight database.
 */
trait LightweightDatabaseAdapterComponent {

  /**
   * @return a lightweight db instance.
   */
  def liteDbAdapter: LightweightDatabaseAdapter

  /**
   * Lightweight db adapter operations.
   */
  trait LightweightDatabaseAdapter {

    /**
     * @return a new db from specified file path.
     */
    def create(path: String): LightweightDatabase

    /**
     * Calculate how many pages a sql query yields.
     */
    def calculatePageCount(totalCount: Int, pageSize: Int) = {
      require(pageSize > 0)
      val hasRemaining = totalCount % pageSize > 0
      val pages = if (totalCount >= pageSize) totalCount / pageSize else 0
      if (hasRemaining) pages + 1 else pages
    }

  }

  /**
   * Encapsulate a db and expose operations to be done on it.
   */
  trait LightweightDatabase extends AutoCloseable {

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
     * Execute a select statement, returning all rows and values.
     */
    def pagedSelect(statement: String, recordsCount: Int, pageSize: Int = -1): DbPagedResultSet

    /**
     * Execute a query statement.
     * @return a result set.
     */
    def query(query: String): ResultSet

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
    def isClosed: Boolean

    /**
     * @return a set of column names for the specified table.
     */
    def getTableColumnNames(tableName: String): Set[String]
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
   * A trait to allow browsing a large result set with paging.
   */
  type DbPagedResultSet = Iterable[DbResultSetPage]

  /**
   * A trait to allow browsing records of a result set with paging.
   */
  type DbResultSetPage = Iterable[Seq[Any]]

  /**
   * A jdbc-based lite db.
   * @param connection the jdbc connection
   */
  protected[this] abstract class JdbcLightweightDatabase(connection: Connection, splog: Splogger) extends LightweightDatabase {

    connection.setAutoCommit(false)

    val orderByChecker = "(order[ ]+by)".r

    protected def newStatement() = {
      val stmt = connection.createStatement()
      stmt.setQueryTimeout(30) // 30 sec. timeout
      stmt.closeOnCompletion()
      stmt
    }

    override def batch(statement: String): DbBatch = new JdbcBatch(connection.prepareStatement(statement))

    override def execute(stmt: String) = {
      connection.setAutoCommit(true)
      newStatement().executeUpdate(stmt)
      connection.setAutoCommit(false)
    }

    override def pagedSelect(statement: String, totalCount: Int, pageSize: Int = 1000): DbPagedResultSet = {
      val stmt = statement.toLowerCase
      if (pageSize < 1) throw new IllegalArgumentException("invalid page size")
      if (orderByChecker.findFirstIn(stmt).isEmpty) throw new IllegalArgumentException("statement should have an order by clause")
      val pageCount = if (totalCount < pageSize) 1 else (totalCount / pageSize) + (if (totalCount % pageSize > 0) 1 else 0)
      splog.debug(s"statement=$statement, totalCount=$totalCount, pageSize=$pageSize, pageCount=$pageCount, pageSize=$pageSize")
      new JdbcDbPagedResultSet(statement, pageCount, pageSize, connection.createStatement())
    }

    override def commit() = connection.commit()

    override def close() = connection.close()

    override def isClosed = connection.isClosed

    override def selectCount(query: String) = {
      val rs = newStatement().executeQuery(query)
      if (rs.next()) rs.getInt(1) else 0
    }

    override def query(query: String): ResultSet = {
      newStatement().executeQuery(query)
    }

  }

  /**
   * A jdbc-based batch processor.
   * @param preparedStatement the jdbc `PreparedStatement`
   */
  private[this] class JdbcBatch(preparedStatement: PreparedStatement) extends DbBatch {

    override def param[T <: Any](index: Int, value: T) = value match {
      case anInt: Int => preparedStatement.setInt(index, anInt)
      case aLong: Long => preparedStatement.setLong(index, aLong)
      case aFloat: Float => preparedStatement.setFloat(index, aFloat)
      case aDouble: Double => preparedStatement.setDouble(index, aDouble)
      case aString: String => preparedStatement.setString(index, aString)
      case aDate: java.sql.Date => preparedStatement.setDate(index, aDate)
      case aTimestamp: java.sql.Timestamp => preparedStatement.setTimestamp(index, aTimestamp)
      case anUnsupportedSqlType => throw new RuntimeException("unsupported type " + value.getClass)
    }

    override def add() = preparedStatement.addBatch()

    override def execute() = preparedStatement.executeBatch()

  }

  private[this] class JdbcDbPagedResultSet(private val sql: String,
                                           private val pageCount: Int,
                                           private val pageSize: Int,
                                           private val statement: Statement) extends DbPagedResultSet {
    private var currentPage = 0

    override val iterator = new Iterator[DbResultSetPage] {

      override def hasNext: Boolean = currentPage < pageCount

      override def next() = {
        val offset = currentPage * pageSize
        statement.execute(sql + s" limit $pageSize offset $offset")
        currentPage += 1
        new JdbcDbResultSetPage(statement.getResultSet)
      }
    }
  }

  private[this] class JdbcDbResultSetPage(private val resultSet: ResultSet) extends DbResultSetPage {

    private val metaData = resultSet.getMetaData

    override val iterator = new Iterator[Seq[Any]] {

      override def hasNext: Boolean = resultSet.next()

      override def next(): Seq[Any] = {
        for {
          i <- 1 to metaData.getColumnCount
        } yield {
          metaData.getColumnType(i) match {
            case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT => resultSet.getInt(i)
            case Types.CLOB | Types.CHAR | Types.VARCHAR | Types.NCHAR | Types.NVARCHAR => resultSet.getString(i)
            case Types.REAL | Types.DOUBLE | Types.FLOAT | Types.NUMERIC | Types.DECIMAL => resultSet.getDouble(i)
            case Types.BOOLEAN => resultSet.getBoolean(i)
            case Types.DATE => resultSet.getString(i)
            case Types.TIME | Types.TIMESTAMP => resultSet.getLong(i)
            case _ => resultSet.getObject(i)
          }
        }
      }
    }
  }

}

/**
 * @inheritdoc
 * sqllite implementation
 */
trait SQLiteLightweightDatabaseAdapterComponent extends LightweightDatabaseAdapterComponent {

  this: SploggerComponent =>

  override val liteDbAdapter: LightweightDatabaseAdapter = new SQLiteLightweightDatabaseAdapter

  private[this] class SQLiteLightweightDatabaseAdapter extends LightweightDatabaseAdapter {

    /* Load driver, make sure it exists */
    Class.forName("org.sqlite.JDBC")

    override def create(path: String): LightweightDatabase = {
      splog.debug(s"Connecting to db path=$path")
      val connection = DriverManager.getConnection(s"jdbc:sqlite:$path")
      val db = new SQLiteLightweightDatabase(connection)
      db.optimize()
      db
    }

  }

  private[this] class SQLiteLightweightDatabase(connection: Connection)
    extends JdbcLightweightDatabase(connection, splog) {

    override def getTableColumnNames(tableName: String): Set[String] = {
      val statement = newStatement()
      val columns = collection.mutable.Set[String]()
      try {
        val columnsResultSet = statement.executeQuery(s"""PRAGMA table_info("$tableName")""")
        while (columnsResultSet.next()) {
          columns += columnsResultSet.getString(2)
        }
      } finally {
        statement.close()
      }
      columns.toSet
    }

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