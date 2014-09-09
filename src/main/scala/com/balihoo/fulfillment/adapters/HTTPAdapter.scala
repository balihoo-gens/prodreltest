package com.balihoo.fulfillment.adapters

import com.stackmob.newman._
import com.stackmob.newman.dsl._
import com.stackmob.newman.response.HttpResponse
import scala.concurrent._
import scala.concurrent.duration._
import java.net.URL

trait HTTPAdapterComponent {
  def httpAdapter: AbstractHTTPAdapter
}

abstract class AbstractHTTPAdapter {
  def delete(url: URL): HttpResponse
  def get(url: URL): HttpResponse
  def post(url: URL, body: AnyRef): HttpResponse
  def put(url: URL, body: AnyRef): HttpResponse
}

class HTTPAdapter(timeoutSeconds: Int) extends AbstractHTTPAdapter {
  private implicit val httpClient = new ApacheHttpClient

  private def execute(builder: Builder) = Await.result(builder.apply, timeoutSeconds.seconds)

  override def delete(url: URL) = execute(DELETE(url))
  override def get(url: URL) = execute(GET(url))
  override def post(url: URL, body: AnyRef) = execute(POST(url).setBody(body))
  override def put(url: URL, body: AnyRef) = execute(PUT(url).setBody(body))
}

class HTTPResult(responseCode: Int, body: String)
