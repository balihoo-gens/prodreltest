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
  def delete(url: URL, headers: List[(String, String)] = List()): HttpResponse
  def get(url: URL, headers: List[(String, String)] = List()): HttpResponse
  def post(url: URL, body: AnyRef, headers: List[(String, String)] = List()): HttpResponse
  def put(url: URL, body: AnyRef, headers: List[(String, String)] = List()): HttpResponse
}

class HTTPAdapter(timeoutSeconds: Int) extends AbstractHTTPAdapter {
  private implicit val httpClient = new ApacheHttpClient

  private def execute(builder: Builder) = Await.result(builder.apply, timeoutSeconds.seconds)

  override def delete(url: URL, headers: List[(String, String)] = List()) = execute(DELETE(url).addHeaders(headers))
  override def get(url: URL, headers: List[(String, String)] = List()) = execute(GET(url).addHeaders(headers))
  override def post(url: URL, body: AnyRef, headers: List[(String, String)] = List()) = execute(POST(url).addHeaders(headers).setBody(body))
  override def put(url: URL, body: AnyRef, headers: List[(String, String)] = List()) = execute(PUT(url).addHeaders(headers).setBody(body))
}
