package com.balihoo.fulfillment.adapters

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model.{DescribeTableResult, TableDescription}
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import org.mockito.Mockito._
import org.mockito.Matchers._

/**
 * This is a place to put stuff commonly used for test classes that extend LoggingWorkflowAdapter.
 *
 * You can't use Mockito via specs2 in a trait file or you'll get a NullPointerException.  This is likely due to a bug
 * in specs2.  As a workaround, we need to call Mockito directly.  That's why this file is the way it is.
 */
trait LoggingWorkflowAdapterTestImpl
  extends LoggingWorkflowAdapter {

  private lazy val _splog = mock(classOf[Splogger])
  def splog = _splog

  /**
   * This is far from ideal because the mock objects defined in this method get recreated every time the swfAdapter is
   * called for.  However, moving the definitions out of this method breaks Mockito because it doesn't understand the
   * non-Java structure of the resulting objects.  If you can figure out a better way to handle this, please fix it.
   * @return a mock AmazonSimpleWorkflowAsyncClient
   */
  def swfAdapter = {
    val _config = mock(classOf[PropertiesLoader])
    when(_config.getString(anyString)).thenReturn("mock")
    when(_config.getString("name")).thenReturn("workername")

    val _client = mock(classOf[AmazonSimpleWorkflowAsyncClient])

    val _swfAdapter = mock(classOf[SWFAdapter])
    when(_swfAdapter.domain).thenReturn("mockdomain")
    when(_swfAdapter.config).thenReturn(_config)
    when(_swfAdapter.client).thenReturn(_client)

    _swfAdapter
  }

  def dynamoAdapter = {
    val _config = mock(classOf[PropertiesLoader])
    when(_config.getString(anyString)).thenReturn("mock")

    val tableResult = new DescribeTableResult
    val tableDesc = new TableDescription
    tableDesc.setTableStatus("ACTIVE")
    tableResult.setTable(tableDesc)
    val _client = mock(classOf[AmazonDynamoDBAsyncClient])
    when(_client.describeTable(anyString)).thenReturn(tableResult)

    val _dynamoAdapter = mock(classOf[DynamoAdapter])
    when(_dynamoAdapter.config).thenReturn(_config)
    when(_dynamoAdapter.client).thenReturn(_client)
    _dynamoAdapter

  }
}
