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
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

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
   * Converts a JSON string into a Target object.  The JSON object has several optional attributes for different types
   * of geographic data, as follows:
   * - countryCodes is an array of two character ISO country codes.
   * - regionIds is an array of Facebook region IDs.  A region is a state, province, or equivalent area.
   * - subregions is an array of names of counties or county equivalent areas, which will be resolved to city IDs by the worker.
   * - cityIds is an array of Facebook city IDs.
   * - cities is an array of city names, which will be resolved to city IDs by the worker.
   * @param jsonString the input
   * @return the output
   */
  private def createTarget(jsonString: String): Target = {
    // Parse the JSON
    val json = Json.parse(jsonString).as[JsObject]
    val countryCodes = (json \ "countryCodes").asOpt[Seq[String]]
    val regionIds = (json \ "regionIds").asOpt[Seq[Int]]
    val subregions = (json \ "subregions").asOpt[Seq[String]]
    val cityIds = (json \ "cityIds").asOpt[Seq[Int]]
    val cityNames = (json \ "cities").asOpt[Seq[String]]

    // If the list of country codes is exactly one element long, that's the country code we'll use for resolving city IDs.
    // Otherwise, we won't be able to resolve city IDs.
    val countryCode = (countryCodes, subregions.getOrElse(Nil), cityNames.getOrElse(Nil)) match {
      case (Some(s :: Nil), _, _) => s // Return the one country code.
      case (_, Nil, Nil) => "" // We could return anything here, because it won't be used without any city or subregion names.
      case _ => throw new IllegalArgumentException("Exactly one country code is required when place names are used.")
    }

    // Resolve countries
    val countries = countryCodes.getOrElse(Seq())

    // Resolve regions
    val regions = regionIds.getOrElse(Seq())

    // Resolve cities
    val cityIdsFromSubregions = subregions.getOrElse(Seq()).map(lookupSubregion(countryCode, _)).flatten
    val cityIdsFromCityNames = cityNames.getOrElse(Seq()).map(lookupCity(countryCode, _)).flatten
    val baseCityIds = cityIds.getOrElse(Seq())
    val cities = baseCityIds ++ cityIdsFromSubregions ++ cityIdsFromCityNames

    // Bundle all the data together into a Target
    new Target(countries, intsToIntegers(regions), intsToIntegers(cities))
  }

  // A regular expression that matches one or more digits
  private val digitsRegEx = "^(\\d+)$".r

  /**
   * Looks up a city by name and returns the Facebook city ID.
   * @param city the city name formatted as "<city name>,<subregion name>,<region code>"
   * @param countryCode the country code
   * @return the city ID or None if the city isn't found
   */
  private def lookupCity(countryCode: String, city: String): Option[Int] = {
    // Split the string on commas
    val stringParts = city.split(",")
    if (stringParts.length < 3) {
      throw new FacebookPosterException(s"Unable to parse city name: $city")
    }

    // Identify the region
    val regionCode = stringParts.last.trim
    val sansRegion = stringParts.init

    // Identity the subregion
    val subregionName = sansRegion.last.trim
    val sansSubregion = sansRegion.init

    // Reassemble whatever's left into the city name.  City names may contain commas.
    val cityName = sansSubregion.mkString(",").trim

    // Query the geo service for the city
    val url = new URL(geoServiceUrl, s"countries/$countryCode/regions/$regionCode/subregions/$subregionName/facebookcities/$cityName")
    val queryResult = httpAdapter.get(url)
    val cityId = (queryResult.code.code, queryResult.bodyString.trim) match {
      case (200, digitsRegEx(id)) if id.toInt > 0 => Some(id.toInt)
      case (200, _) => None // Matches 0 and "null"
      case _ => throw new FacebookPosterException(s"Geo service responded with $queryResult")
    }
    cityId
  }

  /**
   * Looks up a subregion by name and returns all of the city IDs in the subregion.
   * @param subregion the subregion name formatted as "<subregion name>,<region code>"
   * @param countryCode the country code
   * @return the city IDs
   */
  private def lookupSubregion(countryCode: String, subregion: String): Seq[Int] = {
    // Identify the region and subregion
    val splitString = subregion.split(",")
    if (splitString.length != 2) {
      throw new IllegalArgumentException(s"Unable to parse subregion name: $subregion")
    }
    val subregionName = splitString(0).trim
    val regionCode = splitString(1).trim

    // Query the geo service for all cities in the subregion
    val url = new URL(geoServiceUrl, s"countries/$countryCode/regions/$regionCode/subregions/$subregionName/facebookcities")
    val queryResult = httpAdapter.get(url)
    val cityIds = queryResult.code.code match {
      case 200 => Json.parse(queryResult.bodyString).as[Seq[Int]]
      case _ => throw new FacebookPosterException(s"Geo service responded with $queryResult")
    }
    cityIds
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