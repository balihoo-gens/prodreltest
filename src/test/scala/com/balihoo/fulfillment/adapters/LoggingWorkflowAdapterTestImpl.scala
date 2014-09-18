package com.balihoo.fulfillment.adapters

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import org.mockito.Mockito._
import org.mockito.Matchers._

/**
 * This is a place to put stuff commonly used for test classes that extend LoggingWorkflowAdapter.
 *
 * You can't use Mockito the specs2 way in a trait file or you'll get a NullPointerException.  You need to call Mockito
 * directly.  That's why this file is the way it is.
 */
trait LoggingWorkflowAdapterTestImpl
  extends LoggingWorkflowAdapter {

  private lazy val _splog = mock(classOf[Splogger])
  def splog = _splog

  private lazy val _dynamoAdapter = mock(classOf[DynamoAdapter])
  def dynamoAdapter = _dynamoAdapter

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
}
