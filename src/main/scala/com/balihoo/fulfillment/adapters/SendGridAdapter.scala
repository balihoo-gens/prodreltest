package com.balihoo.fulfillment.adapters

import java.net.URL
import com.balihoo.fulfillment.config.{PropertiesLoader, PropertiesLoaderComponent}
import com.balihoo.fulfillment.util.{SploggerComponent, Splogger}
import org.apache.commons.codec.digest.DigestUtils
import play.api.libs.json.{JsValue, JsObject, Json}

trait SendGridAdapterComponent {
  def sendGridAdapter: AbstractSendGridAdapter
}

abstract class AbstractSendGridAdapter {
  this: PropertiesLoaderComponent
    with SploggerComponent
    with HTTPAdapterComponent =>

  lazy val testUser = config.getString("testUser")
  lazy val passwordSalt = config.getString("passwordSalt")
  lazy val v1ApiBaseUrl = new URL(fixUrl(config.getString("v1ApiBaseUrl")))
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

  /**
   * Produces the credentials for a SendGrid subaccount.
   * @param subaccount the subaccount information
   * @return the credentials
   */
  private def getCredentials(subaccount: SendGridSubaccount) = {
    val apiUser = {
      if (subaccount.useTestSubaccount) {
        testUser
      } else {
        "FF" + subaccount.participantId
      }
    }

    // Generate a password that's reproducible, but hard to guess.  The salt makes it hard for people with access to dev
    // and stage configs to guess prod passwords.
    val apiKey = DigestUtils.sha256Hex(apiUser + passwordSalt).substring(0, 16);

    new SendGridCredentials(apiUser, apiKey)
  }

  /**
   * Get the credentials for a subaccount.
   * @param subaccount
   * @return
   */
  implicit def subaccountToCredentials(subaccount: SendGridSubaccount): SendGridCredentials = getCredentials(subaccount)

  /**
   * Converts subaccount credentials into a form needed for query string parameters.
   * @param credentials
   * @return
   */
  implicit def credentialsToQueryParams(credentials: SendGridCredentials): Seq[(String, Any)] = {
    Seq(("api_user", credentials.apiUser), ("api_key", credentials.apiKey))
  }

  /**
   * Checks to see if a SendGrid subaccount exists.
   * @param subaccount the subaccount information
   * @return true if the subaccount exists
   */
  def checkAccountExists(subaccount: SendGridSubaccount): Boolean = {
    splog.debug("Checking for existence of " + subaccount)
    val url = new URL(v1ApiBaseUrl, "profile.get.json")
    val credentials = getCredentials(subaccount)
    val result = httpAdapter.get(url, queryParams = credentials);

    // Get an array of matching profiles from SendGrid.
    val profileArray = result.code.code match {
      case 200 => Json.parse(result.bodyString).asOpt[Seq[JsObject]]
      case _ => throw new SendGridException(s"SendGrid responded with $result")
    }

    // SendGrid with either return a JSON object containing an error message, or a JSON array containing a single
    // object.  Just to be safe, let's check for a single element array where the object has the right username.
    profileArray match {
      case Some(profile :: Nil) if credentials.apiUser == profile.value("username").as[String] => true
      case _ => false
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

case class SendGridSubaccount(participantId: String, useTestSubaccount: Boolean)

case class SendGridCredentials(apiUser: String, apiKey: String)

class SendGridException(message: String) extends Exception(message)
