package com.balihoo.fulfillment.adapters

import java.net.URL
import javax.ws.rs.core._
import javax.ws.rs._
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.{TestProperties, JerseyTest}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import play.api.libs.json.{JsObject, Json}

// This class provides an HTTP server for testing.
@Path("/")
class MockServer extends JerseyTest {

  private class PlainTextWebApplicationException(message: String, status: Response.Status)
    extends WebApplicationException(Response.status(status).entity(message).`type`(MediaType.TEXT_PLAIN).build())

  @DELETE
  @Path("ok")
  def deleteOk = "DELETE OK"

  @DELETE
  @Path("error")
  def deleteError = throw new PlainTextWebApplicationException("DELETE Error", Response.Status.INTERNAL_SERVER_ERROR)

  @GET
  @Path("ok")
  def getOk = "GET OK"

  @GET
  @Path("error")
  def getError = throw new PlainTextWebApplicationException("GET Error", Response.Status.INTERNAL_SERVER_ERROR)

  @POST
  @Path("ok")
  def postOk(body: String) = s"POST OK: $body"

  @POST
  @Path("error")
  def postError(body: String) = throw new PlainTextWebApplicationException(s"POST Error: $body", Response.Status.INTERNAL_SERVER_ERROR)

  @PUT
  @Path("ok")
  def putOk(body: String) = s"PUT OK: $body"

  @PUT
  @Path("error")
  def putError(body: String) = throw new PlainTextWebApplicationException(s"PUT Error: $body", Response.Status.INTERNAL_SERVER_ERROR)

  @GET
  @Path("header")
  def getOk(@Context headers: HttpHeaders) = headers.getHeaderString("testHeader")

  @GET
  @Path("query")
  def getOk(@QueryParam("abc") param1: String, @QueryParam("xyz") param2: Int) = param1 + ":" + param2

  @GET
  @Path("basicauth")
  def getBasicAuth(@Context headers: HttpHeaders) = headers.getHeaderString("Authorization")

  @POST
  @Path("post/basicauth/withtestheader")
  def postBasicAuthWithTestHeader(@Context headers: HttpHeaders, body: String) = {
    val credentials = headers.getHeaderString("Authorization")
    val testHeader = headers.getHeaderString("testHeader")
    s"""
       |{
       |  "credentials": "$credentials",
       |  "testHeader": "$testHeader",
       |  "body": "$body"
       |}
     """.stripMargin
  }

  override def configure = {
    forceSet(TestProperties.CONTAINER_PORT, "0") // Choose first available port.
    new ResourceConfig(getClass)
  }

  def getUrl(relativePath: String) = new URL(getBaseUri.toURL, relativePath)
}

// This class contains the tests.
@RunWith(classOf[JUnitRunner])
class TestHTTPAdapter extends Specification {

  // We'll need an HTTP server.
  val server = new MockServer

  // We'll test an HTTPAdapter with a timeout of 1 second.
  val httpAdapter = new HTTPAdapter(1)

  // Start the HTTP server.
  step {
    server.setUp()
    success
  }

  // Here come the tests.
  "HTTPAdapter" should {
    "DELETE" in {
      val result = httpAdapter.delete(server.getUrl("ok"))
      result.code.code mustEqual 200
      result.bodyString mustEqual "DELETE OK"
    }

    "handle a server error during DELETE" in {
      val result = httpAdapter.delete(server.getUrl("error"))
      result.code.code mustEqual 500
      result.bodyString mustEqual "DELETE Error"
    }

    "GET" in {
      val result = httpAdapter.get(server.getUrl("ok"))
      result.code.code mustEqual 200
      result.bodyString mustEqual "GET OK"
    }

    "handle a server error during GET" in {
      val result = httpAdapter.get(server.getUrl("error"))
      result.code.code mustEqual 500
      result.bodyString mustEqual "GET Error"
    }

    "POST" in {
      val result = httpAdapter.post(server.getUrl("ok"), "One fish")
      result.code.code mustEqual 200
      result.bodyString mustEqual "POST OK: One fish"
    }

    "handle a server error during POST" in {
      val result = httpAdapter.post(server.getUrl("error"), "Two fish")
      result.code.code mustEqual 500
      result.bodyString mustEqual "POST Error: Two fish"
    }

    "PUT" in {
      val result = httpAdapter.put(server.getUrl("ok"), "Red fish")
      result.code.code mustEqual 200
      result.bodyString mustEqual "PUT OK: Red fish"
    }

    "handle a server error during PUT" in {
      val result = httpAdapter.put(server.getUrl("error"), "Blue fish")
      result.code.code mustEqual 500
      result.bodyString mustEqual "PUT Error: Blue fish"
    }

    "set a request header" in {
      val result = httpAdapter.get(server.getUrl("header"), headers = List(("testHeader", "Colorful sashimi buffet")))
      result.code.code mustEqual 200
      result.bodyString mustEqual "Colorful sashimi buffet"
    }

    "prepare a query string" in {
      val result = httpAdapter.get(server.getUrl("query"), queryParams = List(("abc", "yahoo"), ("xyz", 13)))
      result.code.code mustEqual 200
      result.bodyString mustEqual "yahoo:13"
    }

    "perform basic authentication" in {
      val result = httpAdapter.get(server.getUrl("basicauth"), credentials = Some(("superman", "kryptonite")))
      result.code.code === 200
      result.bodyString === "Basic c3VwZXJtYW46a3J5cHRvbml0ZQ=="
    }

    "handle basic authentication with an extra header in a POST" in {
      val url = server.getUrl("post/basicauth/withtestheader")
      val body = "Why did the chicken cross the road?"
      val headers = Seq(("testHeader", "lame joke"))
      val credentials = Some(("superman", "kryptonite"))
      val result = httpAdapter.post(url, body, headers = headers, credentials = credentials)
      result.code.code mustEqual 200
      val json = Json.parse(result.bodyString).as[JsObject]
      json.value("credentials").as[String] === "Basic c3VwZXJtYW46a3J5cHRvbml0ZQ=="
      json.value("testHeader").as[String] === headers.head._2
      json.value("body").as[String] === body
    }
  }

  // Stop the HTTP server.
  step {
    server.tearDown()
    success
  }

}
