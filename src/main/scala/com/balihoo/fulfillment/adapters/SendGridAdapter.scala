package com.balihoo.fulfillment.adapters

import java.net.URL
import com.balihoo.fulfillment.config.{PropertiesLoader, PropertiesLoaderComponent}
import com.balihoo.fulfillment.util.{SploggerComponent, Splogger}
import com.netaporter.uri.encoding.PercentEncoder
import com.stackmob.newman.response.HttpResponse
import org.apache.commons.codec.digest.DigestUtils
import play.api.libs.json.{JsObject, Json}
import com.netaporter.uri.dsl._

trait SendGridAdapterComponent {
  def sendGridAdapter: AbstractSendGridAdapter
}

abstract class AbstractSendGridAdapter {
  this: PropertiesLoaderComponent
    with SploggerComponent
    with HTTPAdapterComponent =>

  lazy val rootApiUser = config.getString("apiUser")
  lazy val rootApiKey = config.getString("apiKey")
  lazy val testUser = config.getString("testUser")
  lazy val passwordSalt = config.getString("passwordSalt")
  lazy val v1ApiBaseUrl = new URL(fixUrl(config.getString("v1ApiBaseUrl")))
  lazy val v2ApiBaseUrl = new URL(fixUrl(config.getString("v2ApiBaseUrl")))
  lazy val v3ApiBaseUrl = new URL(fixUrl(config.getString("v3ApiBaseUrl")))

  /**
   * Ensures that a URL ends with a slash.
   * @param url
   * @return the URL with a slash appended, if needed
   */
  def fixUrl(url: String): String = {
    if (url.endsWith("/")) {
      url
    } else {
      url + "/"
    }
  }

  // Generate a password that's reproducible, but hard to guess.  The salt makes it hard for people with access to dev
  // and stage configs to guess prod passwords.
  private def apiKey(apiUser: String) = DigestUtils.sha256Hex(apiUser + passwordSalt).substring(0, 16);

  /**
   * Produces the credentials for a SendGrid subaccount.  Use this method when you need to determine the username and
   * password.  If you already know the username, use [[getCredentials(String)*]] instead.
   * @param subaccountId the subaccount information
   * @return the credentials
   */
  private def getCredentials(subaccountId: SendGridSubaccountId): SendGridCredentials = {
    val apiUser = {
      if (subaccountId.useTestSubaccount) {
        testUser
      } else {
        "FF" + subaccountId.participantId
      }
    }

   new SendGridCredentials(apiUser, apiKey(apiUser))
  }

  /**
   * Produces the credentials for a SendGrid user.  Use this method when you already know the username and you only need
   * to get the password.  If you need both, use [[getCredentials(SendGridSubaccount)*]] instead.
   * @param apiUser the API user
   * @return the credentials
   */
  def getCredentials(apiUser: String) = new SendGridCredentials(apiUser, apiKey(apiUser))

  /**
   * Get the credentials for a subaccount.
   * @param subaccountId
   * @return
   */
  implicit def subaccountToCredentials(subaccountId: SendGridSubaccountId): SendGridCredentials = getCredentials(subaccountId)

  /**
   * Converts subaccount credentials into a form needed for query string parameters.
   * @param credentials
   * @return
   */
  implicit def credentialsToQueryParams(credentials: SendGridCredentials): Seq[(String, Any)] = {
    Seq(("api_user", credentials.apiUser), ("api_key", credentials.apiKey))
  }

  /**
   * Checks a JsObject for a given key/value pair
   * @param obj
   * @param key
   * @param expectedValue
   * @return true if the key exists and the value matches
   */
  def jsObjectValueEquals(obj: JsObject, key: String, expectedValue: String) =
    obj.keys.contains(key) && obj.value(key).as[String] == expectedValue

  /**
   * Checks to see if a SendGrid subaccount exists.
   * @param subaccountId the subaccount information
   * @return the username for the subaccount, if it exists
   */
  def checkSubaccountExists(subaccountId: SendGridSubaccountId): Option[String] = {
    splog.debug("Checking for existence of " + subaccountId)
    // As of 11/3/14, this doesn't work with the v2 or v3 API.
    val url = new URL(v1ApiBaseUrl, "profile.get.json")
    val credentials = getCredentials(subaccountId)
    val result = httpAdapter.get(url, queryParams = credentials);

    // Get an array of matching profiles from SendGrid.
    val profileArray = result.code.code match {
      case 200 => Json.parse(result.bodyString).asOpt[Seq[JsObject]]
      case _ => throw new SendGridException(s"SendGrid responded with $result")
    }

    // SendGrid with either return a JSON object containing an error message, or a JSON array containing a single
    // object.  Just to be safe, let's check for a single element array where the object has the right username.
    profileArray match {
      case Some(profile :: Nil) if jsObjectValueEquals(profile, "username", credentials.apiUser) => Some(credentials.apiUser)
      case _ => None
    }
  }

  /**
   * Creates a SendGrid subaccount
   * @param subaccount the subaccount information
   */
  def createSubaccount(subaccount: SendGridSubaccount): Unit = {
    splog.debug("Creating subaccount: " + subaccount.credentials.apiUser)
    // As of 11/3/14, this doesn't work with the v3 API.
    val url = new URL(v2ApiBaseUrl, "customer.add.json")
    val queryParams = Seq(
      ("api_user", rootApiUser),
      ("api_key", rootApiKey),
      ("username", subaccount.credentials.apiUser),
      ("password", subaccount.credentials.apiKey),
      ("confirm_password", subaccount.credentials.apiKey),
      ("email", subaccount.email),
      ("first_name", subaccount.firstName),
      ("last_name", subaccount.lastName),
      ("address", subaccount.address),
      ("city", subaccount.city),
      ("state", subaccount.state),
      ("zip", subaccount.zip),
      ("country", subaccount.country),
      ("phone", subaccount.phone),
      ("website", "N/A"))
    val response = httpAdapter.get(url, queryParams = queryParams)
    checkResponseForSuccess(response)
  }

  /**
   * Updates a subaccount's profile
   * @param subaccount The account details
   */
  def updateProfile(subaccount: SendGridSubaccount): Unit = {
    splog.debug("Updating profile for subaccount: " + subaccount.credentials.apiUser)
    val url = new URL(v1ApiBaseUrl, "profile.set.json")
    val queryParams = Seq(
      ("api_user", subaccount.credentials.apiUser),
      ("api_key", subaccount.credentials.apiKey),
      ("first_name", subaccount.firstName),
      ("last_name", subaccount.lastName),
      ("address", subaccount.address),
      ("city", subaccount.city),
      ("state", subaccount.state),
      ("zip", subaccount.zip),
      ("country", subaccount.country),
      ("phone", subaccount.phone),
      ("website", "N/A"))
    val response = httpAdapter.get(url, queryParams = queryParams)
    checkResponseForSuccess(response)
  }

  /**
   * Activates an app for a subaccount
   * @param subaccountUser
   * @param appName
   */
  def activateApp(subaccountUser: String, appName: String): Unit = {
    splog.debug("Activating event notification app for subaccount: " + subaccountUser)
    val url = new URL(v2ApiBaseUrl, "customer.apps.json")
    val queryParams = Seq(
      ("api_user", rootApiUser),
      ("api_key", rootApiKey),
      ("task", "activate"),
      ("user", subaccountUser),
      ("name", appName))
    val response = httpAdapter.get(url, queryParams = queryParams)
    checkResponseForSuccess(response)
  }

  private val credentialEncoder = PercentEncoder(PercentEncoder.GEN_DELIMS ++ PercentEncoder.PATH_CHARS_TO_ENCODE)
  private def encodeCredential(s: String) = credentialEncoder.encode(s, "UTF-8")

  /**
   * Configures the event notification app so it will send all available event data to a given webhook.
   * @param subaccountUser
   * @param webhookUrl
   * @param webhookUser
   * @param webhookPassword
   */
  def configureEventNotificationApp(subaccountUser: String, webhookUrl: String, webhookUser: String, webhookPassword: String): Unit = {
    splog.debug("Configuring event notification app for subaccount: " + subaccountUser)
    val url = new URL(v2ApiBaseUrl, "customer.apps.json")
    // scala-uri doesn't encode the user info correctly, so the username and password are being encoded prior to being
    // added to the URL.  If this bug gets fixed in scala-uri, the encoding step here will need to be removed.
    // See https://github.com/NET-A-PORTER/scala-uri/issues/73
    val urlParam = webhookUrl.withUser(encodeCredential(webhookUser)).withPassword(encodeCredential(webhookPassword)).toString
    val queryParams = Seq(
      ("api_user", rootApiUser),
      ("api_key", rootApiKey),
      ("task", "setup"),
      ("user", subaccountUser),
      ("name", "eventnotify"),
      ("processed", "1"),
      ("dropped", "1"),
      ("deferred", "1"),
      ("delivered", "1"),
      ("bounce", "1"),
      ("click", "1"),
      ("open", "1"),
      ("unsubscribe", "1"),
      ("spamreport", "1"),
      ("url", urlParam))
    val response = httpAdapter.get(url, queryParams = queryParams)
    checkResponseForSuccess(response)
  }

  /**
   * Sets the IP address for a subacount
   * @param subaccountUser
   * @param ipAddress
   */
  def setIpAddress(subaccountUser: String, ipAddress: String): Unit = {
    splog.debug("Setting IP address for subaccount: " + subaccountUser)
    val url = new URL(v2ApiBaseUrl, "customer.sendip.json")
    val queryParams = Seq(
      ("api_user", rootApiUser),
      ("api_key", rootApiKey),
      ("task", "append"),
      ("user", subaccountUser),
      ("set", "specify"),
      ("ip[]", ipAddress))
    val response = httpAdapter.get(url, queryParams = queryParams)
    checkResponseForSuccess(response)
  }

  /**
   * Sets a subaccount's whitelabel
   * @param subaccountUser
   * @param whitelabel
   */
  def setWhitelabel(subaccountUser: String, whitelabel: String): Unit = {
    splog.debug("Setting whitelabel for subaccount: " + subaccountUser)
    val url = new URL(v2ApiBaseUrl, "customer.whitelabel.json")
    val queryParams = Seq(
      ("api_user", rootApiUser),
      ("api_key", rootApiKey),
      ("task", "append"),
      ("user", subaccountUser),
      ("mail_domain", whitelabel))
    val response = httpAdapter.get(url, queryParams = queryParams)
    checkResponseForSuccess(response)
  }

  /**
   * Checks the HTTP response for a 200 code and a SendGrid success message.
   * @param response
   * @throws SendGridException if the code or message are wrong.
   */
  private def checkResponseForSuccess(response: HttpResponse): Unit = {
    // Check the response code
    val bodyOpt = response.code.code match {
      case 200 => Json.parse(response.bodyString).asOpt[JsObject]
      case _ => throw new SendGridException(s"SendGrid responded with $response")
    }

    // Check the response message
    bodyOpt match {
      case Some(body) if jsObjectValueEquals(body, "message", "success") =>
      case _ => throw new SendGridException(s"SendGrid responded with $bodyOpt")
    }
  }
}

class SendGridAdapter(_cfg: PropertiesLoader, _splog: Splogger)
  extends AbstractSendGridAdapter
  with PropertiesLoaderComponent
  with SploggerComponent
  with HTTPAdapterComponent {

  def config = _cfg
  def splog = _splog

  private lazy val _http = new HTTPAdapter(config.getInt("timeoutSeconds"))
  def httpAdapter = _http
}

case class SendGridSubaccountId(participantId: String, useTestSubaccount: Boolean)

case class SendGridCredentials(apiUser: String, apiKey: String)

/**
 * Defines a SendGrid subaccount with all associated data.  The data fields are each truncated to the maximum length
 * that SendGrid will accept, except for the API username.  If the API username is too long, an exception is thrown
 * because the uniqueness of the username can't be guaranteed if it's truncated.
 * @param _credentials
 * @param _firstName
 * @param _lastName
 * @param _address
 * @param _city
 * @param _state
 * @param _zip
 * @param _country
 * @param _phone
 * @throws SendGridException if the API username is too long.
 */
class SendGridSubaccount(_credentials: SendGridCredentials, _firstName: String, _lastName: String, _address: String,
                              _city: String, _state: String, _zip: String, _country: String, _phone: String) {
  val credentials = _credentials
  val email = credentials.apiUser + "@balihoo.com"
  val firstName = _firstName.take(50)
  val lastName = _lastName.take(50)
  val address = _address.take(100)
  val city = _city.take(100)
  val state = _state.take(100)
  val zip = _zip.take(50)
  val country = _country.take(100)
  val phone = _phone.take(50)

  if (email.length > 64) {
    throw new SendGridException("SendGrid API username too long: " + credentials.apiUser)
  }

  override def toString = credentials.apiUser
}

class SendGridException(message: String) extends Exception(message)
