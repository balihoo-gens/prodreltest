package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.NameValuePair
import play.api.libs.json.{Json, JsObject}
import org.apache.commons.io.IOUtils
import java.util
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.utils.URLEncodedUtils

abstract class AbstractGeoNamesTimeZoneRetriever extends FulfillmentWorker {
 this: SWFAdapterComponent with DynamoAdapterComponent =>

  val url = swfAdapter.config.getString("geonames.url")
  val username = swfAdapter.config.getString("geonames.user")
  val token = swfAdapter.config.getString("geonames.token")

  val translator = Map(
    "Pacific/Pohnpei" -> "Pacific/Efate",
    "Pacific/Chuuk" -> "Antarctica/DumontDUrville",
    "Europe/Zaporozhye" -> "Asia/Amman",
    "Europe/Zagreb" -> "Africa/Ceuta",
    "Europe/Volgograd" -> "Europe/Moscow",
    "Europe/Vatican" -> "Africa/Ceuta",
    "Europe/Uzhgorod" -> "Asia/Amman",
    "Europe/Skopje" -> "Africa/Ceuta",
    "Europe/Simferopol" -> "Asia/Amman",
    "Europe/Sarajevo" -> "Africa/Ceuta",
    "Europe/San_Marino" -> "Africa/Ceuta",
    "Europe/Podgorica" -> "Africa/Ceuta",
    "Europe/Mariehamn" -> "Asia/Amman",
    "Europe/Ljubljana" -> "Africa/Ceuta",
    "Europe/Jersey" -> "Africa/Casablanca",
    "Europe/Isle_of_Man" -> "Africa/Casablanca",
    "Europe/Guernsey" -> "Africa/Casablanca",
    "Europe/Bratislava" -> "Africa/Ceuta",
    "Australia/Melbourne" -> "Australia/Hobart",
    "Australia/Lindeman" -> "Antarctica/DumontDUrville",
    "Australia/Currie" -> "Australia/Hobart",
    "Australia/Broken_Hill" -> "Australia/Adelaide",
    "Atlantic/Madeira" -> "Africa/Casablanca",
    "Atlantic/Faroe" -> "Atlantic/Faeroe",
    "Asia/Urumqi" -> "Antarctica/Casey",
    "Asia/Samarkand" -> "Antarctica/Mawson",
    "Asia/Sakhalin" -> "Asia/Vladivostok",
    "Asia/Qyzylorda" -> "Antarctica/Vostok",
    "Asia/Pontianak" -> "Antarctica/Davis",
    "Asia/Oral" -> "Antarctica/Mawson",
    "Asia/Novosibirsk" -> "Asia/Omsk",
    "Asia/Novokuznetsk" -> "Asia/Omsk",
    "Asia/Kuching" -> "Antarctica/Casey",
    "Asia/Kolkata" -> "Asia/Colombo",
    "Asia/Kashgar" -> "Antarctica/Casey",
    "Asia/Ho_Chi_Minh" -> "Antarctica/Davis",
    "Asia/Harbin" -> "Antarctica/Casey",
    "Asia/Chongqing" -> "Antarctica/Casey",
    "Asia/Anadyr" -> "Asia/Kamchatka",
    "Arctic/Longyearbyen" -> "Africa/Ceuta",
    "Antarctica/Macquarie" -> "Pacific/Efate",
    "America/Yakutat" -> "America/Anchorage",
    "America/Thunder_Bay" -> "America/Grand_Turk",
    "America/Swift_Current" -> "America/Belize",
    "America/St_Barthelemy" -> "America/Anguilla",
    "America/Sitka" -> "America/Anchorage",
    "America/Shiprock" -> "America/Denver",
    "America/Santarem" -> "America/Araguaina",
    "America/Santa_Isabel" -> "America/Los_Angeles",
    "America/Resolute" -> "America/Chicago",
    "America/Rankin_Inlet" -> "America/Chicago",
    "America/Rainy_River" -> "America/Chicago",
    "America/Pangnirtung" -> "America/Grand_Turk",
    "America/Ojinaga" -> "America/Denver",
    "America/North_Dakota/New_Salem" -> "America/Chicago",
    "America/North_Dakota/Center" -> "America/Chicago",
    "America/North_Dakota/Beulah" -> "America/Chicago",
    "America/Nome" -> "America/Anchorage",
    "America/Nipigon" -> "America/Grand_Turk",
    "America/Monterrey" -> "America/Chicago",
    "America/Moncton" -> "America/Halifax",
    "America/Metlakatla" -> "Pacific/Pitcairn",
    "America/Merida" -> "America/Chicago",
    "America/Menominee" -> "America/Chicago",
    "America/Matamoros" -> "America/Chicago",
    "America/Marigot" -> "America/Anguilla",
    "America/Kentucky/Monticello" -> "America/Grand_Turk",
    "America/Kentucky/Louisville" -> "America/Grand_Turk",
    "America/Juneau" -> "America/Anchorage",
    "America/Inuvik" -> "America/Denver",
    "America/Indiana/Winamac" -> "America/Grand_Turk",
    "America/Indiana/Vincennes" -> "America/Grand_Turk",
    "America/Indiana/Vevay" -> "America/Grand_Turk",
    "America/Indiana/Tell_City" -> "America/Chicago",
    "America/Indiana/Petersburg" -> "America/Grand_Turk",
    "America/Indiana/Marengo" -> "America/Grand_Turk",
    "America/Indiana/Knox" -> "America/Chicago",
    "America/Indiana/Indianapolis" -> "America/Grand_Turk",
    "America/Goose_Bay" -> "America/Halifax",
    "America/Glace_Bay" -> "America/Halifax",
    "America/Eirunepe" -> "America/Anguilla",
    "America/Detroit" -> "America/Grand_Turk",
    "America/Dawson" -> "America/Los_Angeles",
    "America/Chihuahua" -> "America/Denver",
    "America/Cancun" -> "America/Chicago",
    "America/Cambridge_Bay" -> "America/Denver",
    "America/Boise" -> "America/Denver",
    "America/Blanc-Sablon" -> "America/Anguilla",
    "America/Bahia_Banderas" -> "America/Chicago",
    "America/Atikokan" -> "America/Bogota",
    "America/Argentina/Ushuaia" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/Tucuman" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/San_Luis" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/San_Juan" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/Salta" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/Rio_Gallegos" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/Mendoza" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/La_Rioja" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/Jujuy" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/Cordoba" -> "America/Argentina/Buenos_Aires",
    "America/Argentina/Catamarca" -> "America/Argentina/Buenos_Aires",
    "Africa/Asmara" -> "Africa/Asmera"
  )

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("lat", "float", ""),
      new ActivityParameter("lon", "float", "")
    ), new ActivityResult("string", "Timezone like (America/Denver)"))
  }

  override def handleTask(params: ActivityParameters) = {

    val lat = params("lat")
    val lon = params("lon")
    val results = latLonToTimeZone(lat, lon)

    val jresults = Json.parse(results).as[JsObject]

    if(!(jresults.keys contains "timezoneId")) {
      throw new Exception(results+" does not contain 'timezoneId'")
    }

    completeTask(translateZone(jresults.value("timezoneId").as[String]))
  }

  def latLonToTimeZone(lat:String, lon:String) = {

    val params = new util.LinkedList[NameValuePair]
    params.add(new BasicNameValuePair("lat", lat))
    params.add(new BasicNameValuePair("lng", lon))
    params.add(new BasicNameValuePair("username", username))
    params.add(new BasicNameValuePair("token", token))

    val httpget = new HttpGet(url+"/timezoneJSON?"+URLEncodedUtils.format(params, "utf-8"))
    val client = new DefaultHttpClient
    val response = client.execute(httpget)
    IOUtils.toString(response.getEntity.getContent)
  }

  def translateZone(zone:String) = {
    translator.getOrElse(zone, zone)
  }

}

class GeoNamesTimeZoneRetriever(swf: SWFAdapter, dyn: DynamoAdapter)
  extends AbstractGeoNamesTimeZoneRetriever
  with SWFAdapterComponent
  with DynamoAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
}

object geonames_timezoneretriever {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val cfg = PropertiesLoader(args, name)
    val worker = new GeoNamesTimeZoneRetriever(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg)
    )
    println(s"Running $name")
    worker.work()
  }
}
