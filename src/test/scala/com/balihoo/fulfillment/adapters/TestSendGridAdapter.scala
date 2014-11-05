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
      sendGridAdapter.checkSubaccountExists(realSubaccountId) must beSome(realSubaccountCredentials.apiUser)
    }

    "verify that a test subaccount exists" in new adapter {
      sendGridAdapter.checkSubaccountExists(testSubaccountId) must beSome(testSubaccountUser)
    }

    "verify that a bogus subaccount doesn't exist" in new adapter {
      sendGridAdapter.checkSubaccountExists(bogusSubaccountId) must beNone
    }

    "handle a bad response when checking for a subaccount" in new adapter {
      sendGridAdapter.checkSubaccountExists(errorSubaccountId1) must throwA[SendGridException]
    }

    "create a real subaccount" in new adapter {
      sendGridAdapter.createSubaccount(realSubaccount) === realSubaccountCredentials.apiUser
    }

    "create a test subaccount" in new adapter {
      sendGridAdapter.createSubaccount(testSubaccount) === testSubaccountUser
    }

    "handle a bad response when creating a subaccount" in new adapter {
      sendGridAdapter.createSubaccount(errorSubaccount1) must throwA[SendGridException]
    }

    "handle an error response when creating a subaccount" in new adapter {
      sendGridAdapter.createSubaccount(errorSubaccount2) must throwA[SendGridException]
    }
  }
}

trait adapter extends Scope with Mockito {
  val testRootApiUser = "mainAccountUser"
  val testRootApiKey = "StinkyCheese"
  val testSubaccountUser = "drunkenGorilla"
  val testPasswordSalt = "iodized"
  val v1ApiBaseUrlString = "https://whats.up/doc" // Don't put a slash at the end of this URL, or the tests won't work.
  val v2ApiBaseUrlString = "https://less.old/api" // Same with this one.
  val v3ApiBaseUrlString = "https://hot.new/api" // Same with this one.
  val profileGetUrl = new URL(v1ApiBaseUrlString / "profile.get.json")
  val createSubaccountUrl = new URL(v2ApiBaseUrlString / "customer.add.json")
  val realSubaccountParticipantId = "12345"
  val bogusSubaccountParticipantId = "manEatingBanana"
  val errorSubaccountParticipantId1 = "indigestion1"
  val errorSubaccountParticipantId2 = "indigestion2"
  val realSubaccountId = SendGridSubaccountId(realSubaccountParticipantId, false)
  val testSubaccountId = SendGridSubaccountId(realSubaccountParticipantId, true)
  val bogusSubaccountId = SendGridSubaccountId(bogusSubaccountParticipantId, false)
  val errorSubaccountId1 = SendGridSubaccountId(errorSubaccountParticipantId1, false)
  val apiCredentials = SendGridCredentials(testRootApiUser, testRootApiKey)
  val testSubAccountCredentials = apiUserToCredentials(testSubaccountUser)
  val realSubaccountCredentials = apiUserToCredentials(s"FF$realSubaccountParticipantId")
  val bogusSubaccountCredentials = apiUserToCredentials(s"FF$bogusSubaccountParticipantId")
  val errorSubaccount1Credentials = apiUserToCredentials(s"FF$errorSubaccountParticipantId1")
  val errorSubaccount2Credentials = apiUserToCredentials(s"FF$errorSubaccountParticipantId2")

  val realSubaccount = new SendGridSubaccount(
    _credentials = realSubaccountCredentials,
    _firstName = "Boba",
    _lastName = "Fett",
    _address = "1138 Imperial Way",
    _city = "Great Pit of Carkoon",
    _state = "Tatooine",
    _zip = "0982340980498",
    _country = "a galaxy far, far away",
    _phone = "867-5309")

  val testSubaccount = new SendGridSubaccount(
    _credentials = testSubAccountCredentials,
    _firstName = "Test",
    _lastName = "User",
    _address = "404 South 8th Street Suite 300",
    _city = "Boise",
    _state = "ID",
    _zip = "83702",
    _country = "USA",
    _phone = "2086294254")

  val errorSubaccount1 = new SendGridSubaccount(
    _credentials = errorSubaccount1Credentials,
    _firstName = "Test",
    _lastName = "User",
    _address = "404 South 8th Street Suite 300",
    _city = "Boise",
    _state = "ID",
    _zip = "83702",
    _country = "USA",
    _phone = "2086294254")

  val errorSubaccount2 = new SendGridSubaccount(
    _credentials = errorSubaccount2Credentials,
    _firstName = "Test",
    _lastName = "User",
    _address = "404 South 8th Street Suite 300",
    _city = "Boise",
    _state = "ID",
    _zip = "83702",
    _country = "USA",
    _phone = "2086294254")

  val sendGridAdapter = new AbstractSendGridAdapter
    with PropertiesLoaderComponent
    with SploggerComponent
    with HTTPAdapterComponent {

    val config = mock[PropertiesLoader]
    config.getString("apiUser") returns testRootApiUser
    config.getString("apiKey") returns testRootApiKey
    config.getString("testUser") returns testSubaccountUser
    config.getString("passwordSalt") returns testPasswordSalt
    config.getString("v1ApiBaseUrl") returns v1ApiBaseUrlString
    config.getString("v2ApiBaseUrl") returns v2ApiBaseUrlString
    config.getString("v3ApiBaseUrl") returns v3ApiBaseUrlString

    val splog = mock[Splogger]
    
    val successResponse = buildHttpResponse(200,
      """
        |{
        |  "message": "success"
        |}
      """.stripMargin)
    
    val permissionErrorResponse = buildHttpResponse(200,
      """
        |{
        |  "error": {
        |    "code": 401,
        |    "message": "Permission denied, wrong credentials"
        |  }
        |}
      """.stripMargin)
    
    val serverErrorResponse = buildHttpResponse(500, "Server is drunk")

    // ----------------- Begin mock HttpAdapter -------------------- //
    val httpAdapter = mock[HTTPAdapter]

    // Profile lookup
    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams(testSubAccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns buildHttpResponse(200,
      s"""
        |[
        |  {
        |    "username": "$testSubaccountUser",
        |    "email": "$testSubaccountUser@balihoo.com",
        |    "active": "true",
        |    "first_name": "Test",
        |    "last_name": "User",
        |    "address": "404 South 8th Street Suite 300",
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
    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams(realSubaccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns buildHttpResponse(200,
      s"""
      |[
      |  {
      |    "username": "FF$realSubaccountParticipantId",
      |    "email": "FF$realSubaccountParticipantId@balihoo.com",
      |    "active": "true",
      |    "first_name": "Test",
      |    "last_name": "User",
      |    "address": "404 South 8th Street Suite 300",
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
    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams(bogusSubaccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(profileGetUrl), ===(buildQueryParams(errorSubaccount1Credentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // Subaccount creation
    httpAdapter.get(===(createSubaccountUrl), ===(buildQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(createSubaccountUrl), ===(buildQueryParams(testSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(createSubaccountUrl), ===(buildQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(createSubaccountUrl), ===(buildQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse
    // ----------------- End mock HttpAdapter -------------------- //

    /**
     * Builds a list of query parameters, starting with credentials
     * @param credentials
     * @param otherParams
     * @return
     */
    private def buildQueryParams(credentials: SendGridCredentials, otherParams: Seq[(String, Any)] = Seq()): Seq[(String, Any)] =
      Seq(("api_user", credentials.apiUser), ("api_key", credentials.apiKey)) ++ otherParams

    /**
     * Builds a list of query parameters for creating a subaccount
     * @param subaccount
     * @return
     */
    private def buildQueryParams(subaccount: SendGridSubaccount): Seq[(String, Any)] = {
      buildQueryParams(apiCredentials,  Seq(("username", subaccount.credentials.apiUser),
        ("password", subaccount.credentials.apiKey), ("confirm_password", subaccount.credentials.apiKey),
        ("email", subaccount.email), ("first_name", subaccount.firstName), ("last_name", subaccount.lastName),
        ("address", subaccount.address), ("city", subaccount.city), ("state", subaccount.state), ("zip", subaccount.zip),
        ("country", subaccount.country), ("phone", subaccount.phone), ("website", "N/A")))
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

  /**
   * Creates SendGridCredentials for a give username
   * @param apiUser
   * @return
   */
  def apiUserToCredentials(apiUser: String): SendGridCredentials = {
    val apiKey = DigestUtils.sha256Hex(apiUser + testPasswordSalt).substring(0, 16);
    SendGridCredentials(apiUser, apiKey)
  }

}

private class QueryParamMatcher(apiUser: String, passwordSalt: String, otherParams: Seq[(String, Any)] = Seq()) extends BaseMatcher[Seq[(String, Any)]] {
  val apiKey = DigestUtils.sha256Hex(apiUser + passwordSalt).substring(0, 16);

  override def matches(p1: scala.Any): Boolean = true

  override def describeTo(p1: Description): Unit = p1.appendText(" is ok")
}
