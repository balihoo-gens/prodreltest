package com.balihoo.fulfillment.adapters

import com.stackmob.newman._
import com.stackmob.newman.dsl._
import com.stackmob.newman.response.HttpResponse
import scala.concurrent._
import scala.concurrent.duration._
import java.net.URL
import com.netaporter.uri.dsl._

trait HTTPAdapterComponent {
  def httpAdapter: AbstractHTTPAdapter
}

abstract class AbstractHTTPAdapter {
  def delete(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()): HttpResponse
  def get(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()): HttpResponse
  def post(url: URL, body: AnyRef, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()): HttpResponse
  def put(url: URL, body: AnyRef, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()): HttpResponse

  /**
   * Builds a query string from a list of parameters and appends it to a URL.  If the URL already includes a query string,
   * the parameters will be added to the existing query string.
   * @param url
   * @param params
   * @return
   */
  protected def appendQueryString(url: URL, params: Seq[(String, Any)]): URL = new URL(params.foldLeft(url.toString)((s, param) => s ? param))
}

class HTTPAdapter(timeoutSeconds: Int) extends AbstractHTTPAdapter {
  private implicit val httpClient = new ApacheHttpClient

  private def execute(builder: Builder) = Await.result(builder.apply, timeoutSeconds.seconds)

  override def delete(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()) =
    execute(DELETE(appendQueryString(url, queryParams)).addHeaders(headers.toList))
  override def get(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()) =
    execute(GET(appendQueryString(url, queryParams)).addHeaders(headers.toList))
  override def post(url: URL, body: AnyRef, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()) =
    execute(POST(appendQueryString(url, queryParams)).addHeaders(headers.toList).setBody(body))
  override def put(url: URL, body: AnyRef, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq()) =
    execute(PUT(appendQueryString(url, queryParams)).addHeaders(headers.toList).setBody(body))
}
