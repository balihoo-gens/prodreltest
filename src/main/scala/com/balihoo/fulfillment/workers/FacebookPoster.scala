package com.balihoo.fulfillment.workers

import java.net.URL
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import com.balihoo.socialmedia.facebook.FacebookConnection
import com.balihoo.socialmedia.facebook.model.Target
import org.apache.commons.io.{FilenameUtils, IOUtils}
import play.api.libs.json._
import scala.collection.JavaConversions._

/**
 * Typical workflow
 * ****************
 * 1. Use a FacebookPoster worker to validate the post data.  This should happen as soon as possible after the order is
 * created.
 *
 * 2. Use a FacebookPoster worker to publish the post at the desired time.
 */
abstract class AbstractFacebookPoster extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with FacebookAdapterComponent
  with HTTPAdapterComponent =>

  val _cfg: PropertiesLoader

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("appId", "string", "The Facebook app ID"),
      new ActivityParameter("appSecret", "string", "The Facebook app secret", sensitive = true),
      new ActivityParameter("accessToken", "string", "The Facebook access token", sensitive = true),
      new ActivityParameter("postType", "string", "\"link\", \"photo\", or \"status update\""),
      new ActivityParameter("pageId", "string", "The Facebook page ID"),
      new ActivityParameter("target", "JSON", "The targeting data"),
      new ActivityParameter("message", "string", "The message to post", required = false),
      new ActivityParameter("linkUrl", "string", "A link to include in the post", required = false),
      new ActivityParameter("photoUrl", "string", "The URL of the photo to include in the post", required = false),
      new ActivityParameter("action", "string", "\"validate\" or \"publish\"")
    ), new ActivityResult("string", "the Facebook post ID if the action is \"publish\", otherwise ignore this value"))
  }

  /**
   * Gets the filename of an optional URL.
   * @param url The URL
   * @return The name of the file referenced by the URL, or null if there's no URL
   */
  private def getPhotoName(url: Option[String]): String = {
    url match {
      case Some(u) => FilenameUtils.getName(u)
      case None => null
    }
  }

  /**
   * Downloads an optional photo and turns it into a byte array.
   * @param url The photo's URL
   * @return the file contents, or null if there's no URL
   */
  private def getPhotoBytes(url: Option[String]): Array[Byte] = {
    url match {
      case Some(u) => {
        splog.info(s"Facebook poster downloading $u")
        IOUtils.toByteArray(new URL(u))
      }
      case None => null
    }
  }

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      val appId = params("appId")
      val appSecret = params("appSecret")
      val accessToken = params("accessToken")
      val connection = new FacebookConnection(appId, appSecret, accessToken)
      val postType = params("postType")
      val pageId = params("pageId")
      val target = createTarget(params("target"))
      val message = params.getOrElse("message", null)
      lazy val linkUrl = params.getOrElse("linkUrl", null)
      lazy val photoUrl = params.get("photoUrl")
      lazy val photoBytes = getPhotoBytes(photoUrl)
      lazy val photoName = getPhotoName(photoUrl)
      val action = params("action")
      splog.info(s"Facebook poster was asked to $action a $postType. The page ID is $pageId.")

      (action, postType) match {
        case ("validate", "link") => facebookAdapter.validateLinkPost(connection, pageId, target, linkUrl, message); "OK"
        case ("validate", "photo") => facebookAdapter.validatePhotoPost(connection, pageId, target, photoBytes, photoName, message); "OK"
        case ("validate", "status update") => facebookAdapter.validateStatusUpdate(connection, pageId, target, message); "OK"
        case ("publish", "link") => facebookAdapter.publishLinkPost(connection, pageId, target, linkUrl, message)
        case ("publish", "photo") => facebookAdapter.publishPhotoPost(connection, pageId, target, photoBytes, photoName, message)
        case ("publish", "status update") => facebookAdapter.publishStatusUpdate(connection, pageId, target, message)
        case _ => throw new IllegalArgumentException(s"Invalid action or post type: $action $postType")
      }
    }
  }

  // We'll use this in a couple of places to convert Ints to Integers so the Java libraries can use them.
  private def intsToIntegers(input: Seq[Int]): Seq[Integer] = input.map(Integer.valueOf(_))

  /**
   * Converts a JSON string into a Target object.
   * @param jsonString the input
   * @return the output
   */
  private def createTarget(jsonString: String): Target = {
    // Parse the JSON
    val json = Json.parse(jsonString).as[JsObject]
    val countryCodes = (json \ "countryCodes").as[Seq[String]]
    val regionIds = (json \ "regionIds").as[Seq[Int]]
    val subregions = (json \ "subregions").as[Seq[String]]
    val cityIds = (json \ "cityIds").as[Seq[Int]]
    val cities = (json \ "cities").as[Seq[String]]

    // If the list of country codes is exactly one element long, that's the country code we'll use for resolving city IDs.
    // Otherwise, we won't be able to resolve city IDs.
    val countryCode = countryCodes match {
      case s :: Nil => Some(s)
      case _ => None
    }

    // Resolve subregions and cities into city IDs
    val subregionCityIds = subregions.map(lookupSubregion(countryCode, _)).flatten
    val cityCityIds = cities.map(lookupCity(countryCode, _)).flatten
    val allCities = cityIds ++ subregionCityIds ++ cityCityIds

    // Bundle all the data together into a Target
    new Target(countryCodes, intsToIntegers(regionIds), intsToIntegers(allCities))
  }

  // A regular expression that matches one or more digits
  private val digitsRegEx = "\\d+".r

  /**
   * Looks up a city by name and returns the Facebook city ID.
   * @param city the city name formatted as "<city name>,<subregion name>,<region code>"
   * @param countryCodeOption the country code as an Option
   * @return the city ID or None if the city isn't found
   */
  private def lookupCity(countryCodeOption: Option[String], city: String): Option[Int] = {
    // Do we have a country code?
    val countryCode = countryCodeOption match {
      case Some(s) => s
      case _ => throw new IllegalArgumentException("Unable to determine the country code")
    }

    // Split the string on commas
    val splitString = city.split(",")

    // Identify the region
    val regionCode = splitString.last
    val sansRegion = splitString.init

    // Identity the subregion
    val subregionName = sansRegion.last
    val sansSubregion = sansRegion.init

    // Reassemble whatever's left into the city name.  City names may contain commas.
    val cityName = sansSubregion.mkString(",")

    // Query the geo service for the city
    val url = new URL(geoServiceUrl, s"countries/$countryCode/regions/$regionCode/subregions/$subregionName/facebookcities/$cityName")
    val result = httpAdapter.get(url)
    (result.code.code, result.bodyString) match {
      case (200, digitsRegEx(id)) => Some(id.toInt)
      case (200, _) => None // Handle non-numeric string response ("null")
      case _ => throw new FacebookPosterException(s"Geo service responded with $result")
    }
  }

  /**
   * Looks up a subregion by name and returns all of the city IDs in the subregion.
   * @param subregion the subregion name formatted as "<subregion name>,<region code>"
   * @param countryCodeOption the country code as an Option
   * @return the city IDs
   */
  private def lookupSubregion(countryCodeOption: Option[String], subregion: String): Seq[Int] = {
    // Do we have a country code?
    val countryCode = countryCodeOption match {
      case Some(s) => s
      case _ => throw new IllegalArgumentException("Unable to determine the country code")
    }

    // Identify the region and subregion
    val splitString = subregion.split(",")
    if (splitString.length != 2) {
      throw new IllegalArgumentException(s"Unable to parse subregion: $subregion")
    }
    val subregionName = splitString(1).trim
    val regionCode = splitString(1).trim

    // Query the geo service for all cities in the subregion
    val url = new URL(geoServiceUrl, s"countries/$countryCode/regions/$regionCode/subregions/$subregionName/facebookcities")
    val result = httpAdapter.get(url)
    result.code.code match {
      case 200 => Json.parse(result.bodyString).as[Seq[Int]]
      case _ => throw new FacebookPosterException(s"Geo service responded with $result")
    }
  }

  private def geoServiceUrl = new URL(_cfg.getString("geoServiceUrl"))
}

class FacebookPoster(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractFacebookPoster
  with LoggingWorkflowAdapterImpl
  with FacebookAdapterComponent
  with HTTPAdapterComponent {

  private lazy val _facebookAdapter = new FacebookAdapter
  def facebookAdapter = _facebookAdapter

  private lazy val _http = new HTTPAdapter(_cfg.getInt("geoServiceTimeoutSeconds"))
  def httpAdapter = _http
}

object facebook_poster extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new FacebookPoster(cfg, splog)
  }
}

class FacebookPosterException(message: String) extends Exception(message)
