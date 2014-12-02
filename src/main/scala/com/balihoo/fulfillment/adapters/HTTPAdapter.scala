package com.balihoo.fulfillment.adapters

import com.stackmob.newman._
import com.stackmob.newman.dsl._
import com.stackmob.newman.response.HttpResponse
import sun.misc.BASE64Encoder
import scala.concurrent._
import scala.concurrent.duration._
import java.net.{URLEncoder, URL}
import com.netaporter.uri.dsl._

trait HTTPAdapterComponent {
  def httpAdapter: AbstractHTTPAdapter
}

abstract class AbstractHTTPAdapter {
  def delete(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None): HttpResponse
  def get(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None): HttpResponse
  def post(url: URL, body: String, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None): HttpResponse
  def put(url: URL, body: String, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None): HttpResponse

  /**
   * Builds a query string from a list of parameters and appends it to a URL.  If the URL already includes a query string,
   * the parameters will be added to the existing query string.
   * @param url
   * @param params
   * @return
   */
  protected def appendQueryString(url: URL, params: Seq[(String, Any)]): URL = {
    // Convert to a URI before the foldLeft operation to avoid a bug that shows up if the URL is in string form.
    // https://github.com/NET-A-PORTER/scala-uri/issues/72
    val uri: com.netaporter.uri.Uri = url.toString
    val resultUri = params.foldLeft(uri)((s, param) => s ? param)
    new URL(resultUri.toString)
  }
}

class HTTPAdapter(timeoutSeconds: Int) extends AbstractHTTPAdapter {
  private implicit val httpClient = new ApacheHttpClient
  private val base64Encoder = new BASE64Encoder

  /**
   * Synchronously executes the operation specified in the builder.
   * @param builder
   * @param headers
   * @param credentials optional credentials for basic authentication (username, password)
   * @return
   */
  private def execute(builder: Builder, headers: Seq[(String, String)], credentials: Option[(String, String)]) = {
    val fullHeaders = credentials match {
      case Some((username, password)) => Seq(("Authorization", "Basic " + base64Encoder.encode(s"$username:$password".getBytes))) ++ headers
      case _ => headers
    }
    Await.result(builder.addHeaders(fullHeaders.toList).apply, timeoutSeconds.seconds)
  }

  override def delete(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None) =
    execute(DELETE(appendQueryString(url, queryParams)), headers, credentials)
  override def get(url: URL, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None) =
    execute(GET(appendQueryString(url, queryParams)), headers, credentials)
  override def post(url: URL, body: String, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None) =
    execute(POST(appendQueryString(url, queryParams)).setBody(body), headers, credentials)
  override def put(url: URL, body: String, queryParams: Seq[(String, Any)] = Seq(), headers: Seq[(String, String)] = Seq(), credentials: Option[(String, String)] = None) =
    execute(PUT(appendQueryString(url, queryParams)).setBody(body), headers, credentials)
}

object HTTPAdapter {
  val formContentTypeHeader = "Content-Type" -> "application/x-www-form-urlencoded"

  /**
   * Encodes form data for use as a request body.
   * @param formData key/value pairs
   * @param charset defaults to UTF-8
   * @return
   */
  def encodeFormData(formData: Seq[(String, Any)], charset: String = "UTF-8"): String = {
    def encodeTuple(t: (String, Any)) = URLEncoder.encode(t._1, charset) + "=" + URLEncoder.encode(t._2.toString, charset)

    if (formData.isEmpty) ""
    else formData.map(encodeTuple).reduce { (x, y) => x + "&" + y }
  }
}
