package com.balihoo.fulfillment.adapters

import org.specs2.matcher.AnyMatchers._
import org.specs2.mock._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope
import com.balihoo.fulfillment.config.{PropertiesLoader, PropertiesLoaderComponent}
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import com.stackmob.newman.response.{HttpResponse, HttpResponseCode}
import org.apache.commons.codec.digest.DigestUtils
import org.junit.runner.RunWith
import java.net.URL
import com.netaporter.uri.dsl._
import org.hamcrest.{Description, BaseMatcher}

@RunWith(classOf[JUnitRunner])
class TestSendGridAdapter extends Specification {
  "SendGridAdapter" should {
    "verify that a real subaccount exists" in new adapter {
      sendGridAdapter.checkAccountExists(realSubaccount) must beTrue
    }

    "verify that a test subaccount exists" in new adapter {
      sendGridAdapter.checkAccountExists(testSubaccount) must beTrue
    }

    "verify that a bogus subaccount doesn't exist" in new adapter {
      sendGridAdapter.checkAccountExists(bogusSubaccount) must beFalse
    }

    "handle a bad response when checking for a subaccount" in new adapter {
      sendGridAdapter.checkAccountExists(errorSubaccount) must throwA[SendGridException]
    }
  }
}

trait adapter extends Scope with Mockito {
  val testSubaccountUser = "drunkenGorilla"
  val testPasswordSalt = "iodized"
  val v1ApiBaseUrlString = "https://whats.up/doc" // Don't put a slash at the end of this URL, or the tests won't work.
  val v3ApiBaseUrlString = "https://hot.new/api" // Same with this one.
  val profileGetUrl = new URL(v1ApiBaseUrlString / "profile.get.json")
  val realSubaccountParticipantId = "12345"
  val bogusSubaccountParticipantId = "manEatingBanana"
  val errorSubaccountParticipantId = "indigestion"
  val realSubaccount = SendGridSubaccount(realSubaccountParticipantId, false)
  val testSubaccount = SendGridSubaccount(realSubaccountParticipantId, true)
  val bogusSubaccount = SendGridSubaccount(bogusSubaccountParticipantId, false)
  val errorSubaccount = SendGridSubaccount(errorSubaccountParticipantId, false)

  val sendGridAdapter = new AbstractSendGridAdapter
    with PropertiesLoaderComponent
    with SploggerComponent
    with HTTPAdapterComponent {

    val config = mock[PropertiesLoader]
    config.getString("testUser") returns testSubaccountUser
    config.getString("passwordSalt") returns testPasswordSalt
    config.getString("v1ApiBaseUrl") returns v1ApiBaseUrlString
    config.getString("v3ApiBaseUrl") returns v3ApiBaseUrlString

    val splog = mock[Splogger]

    val httpAdapter = mock[HTTPAdapter]

    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams(testSubaccountUser)), any[Seq[(String, String)]]) returns buildHttpResponse(200,
      """
        |[
        |  {
        |    "username": "drunkenGorilla",
        |    "email": "drunkenGorilla@balihoo.com",
        |    "active": "true",
        |    "first_name": "Test",
        |    "last_name": "User",
        |    "address": "404 South 8th Street",
        |    "address2": "Suite 300",
        |    "city": "Boise",
        |    "state": "ID",
        |    "zip": "83702",
        |    "country": "US",
        |    "phone": "2086294254",
        |    "website": "http://www.balihoo.com",
        |    "website_access": "true"
        |  }
        |]
        |""".stripMargin)

    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams("FF" + realSubaccountParticipantId)), any[Seq[(String, String)]]) returns buildHttpResponse(200,
      """
      |[
      |  {
      |    "username": "FF12345",
      |    "email": "FF12345@balihoo.com",
      |    "active": "true",
      |    "first_name": "Test",
      |    "last_name": "User",
      |    "address": "404 South 8th Street",
      |    "address2": "Suite 300",
      |    "city": "Boise",
      |    "state": "ID",
      |    "zip": "83702",
      |    "country": "US",
      |    "phone": "2086294254",
      |    "website": "http://www.balihoo.com",
      |    "website_access": "true"
      |  }
      |]
      |""".stripMargin)

    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams("FF" + bogusSubaccountParticipantId)), any[Seq[(String, String)]]) returns buildHttpResponse(200,
      """
        |{
        |  "error": {
        |    "code": 401,
        |    "message": "Permission denied, wrong credentials"
        |  }
        |}
      """.stripMargin)

    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams("FF" + errorSubaccountParticipantId)), any[Seq[(String, String)]]) returns buildHttpResponse(500, "")

    /**
     * Builds a list of query parameters
     * @param apiUser
     * @param otherParams
     * @return
     */
    private def buildQueryParams(apiUser: String, otherParams: Seq[(String, Any)] = Seq()): Seq[(String, Any)] = {
      val apiKey = DigestUtils.sha256Hex(apiUser + testPasswordSalt).substring(0, 16);
      Seq(("api_user", apiUser), ("api_key", apiKey)) ++ otherParams
    }

    /**
     * Factory method for mock HttpResponse objects
     * @param code the HTTP response code
     * @param body the body of the response
     * @return
     */
    private def buildHttpResponse(code: Int, body: String) = {
      val responseCode = mock[HttpResponseCode]
      responseCode.code returns code

      val response = mock[HttpResponse]
      response.code returns responseCode
      response.bodyString returns body

      response
    }
  }
}

private class QueryParamMatcher(apiUser: String, passwordSalt: String, otherParams: Seq[(String, Any)] = Seq()) extends BaseMatcher[Seq[(String, Any)]] {
  val apiKey = DigestUtils.sha256Hex(apiUser + passwordSalt).substring(0, 16);

  override def matches(p1: scala.Any): Boolean = true

  override def describeTo(p1: Description): Unit = p1.appendText(" is ok")
}
