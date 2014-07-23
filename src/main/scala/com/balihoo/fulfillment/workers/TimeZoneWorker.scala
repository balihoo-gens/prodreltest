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

abstract class TimeZoneWorker extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {

  val url = swfAdapter.config.getString("geonames.url")
  val username = swfAdapter.config.getString("geonames.user")
  val token = swfAdapter.config.getString("geonames.token")

  val translator = Map(
    "Pacific/Wallis" -> "Pacific/Wallis",
    "Pacific/Wake" -> "Pacific/Wake",
    "Pacific/Tongatapu" -> "Pacific/Tongatapu",
    "Pacific/Tarawa" -> "Pacific/Tarawa",
    "Pacific/Tahiti" -> "Pacific/Tahiti",
    "Pacific/Saipan" -> "Pacific/Saipan",
    "Pacific/Rarotonga" -> "Pacific/Rarotonga",
    "Pacific/Port_Moresby" -> "Pacific/Port_Moresby",
    "Pacific/Pohnpei" -> "Pacific/Efate",
    "Pacific/Pitcairn" -> "Pacific/Pitcairn",
    "Pacific/Palau" -> "Pacific/Palau",
    "Pacific/Pago_Pago" -> "Pacific/Pago_Pago",
    "Pacific/Noumea" -> "Pacific/Noumea",
    "Pacific/Norfolk" -> "Pacific/Norfolk",
    "Pacific/Niue" -> "Pacific/Niue",
    "Pacific/Nauru" -> "Pacific/Nauru",
    "Pacific/Midway" -> "Pacific/Midway",
    "Pacific/Marquesas" -> "Pacific/Marquesas",
    "Pacific/Majuro" -> "Pacific/Majuro",
    "Pacific/Kwajalein" -> "Pacific/Kwajalein",
    "Pacific/Kosrae" -> "Pacific/Kosrae",
    "Pacific/Kiritimati" -> "Pacific/Kiritimati",
    "Pacific/Johnston" -> "Pacific/Johnston",
    "Pacific/Honolulu" -> "Pacific/Honolulu",
    "Pacific/Guam" -> "Pacific/Guam",
    "Pacific/Guadalcanal" -> "Pacific/Guadalcanal",
    "Pacific/Gambier" -> "Pacific/Gambier",
    "Pacific/Galapagos" -> "Pacific/Galapagos",
    "Pacific/Funafuti" -> "Pacific/Funafuti",
    "Pacific/Fiji" -> "Pacific/Fiji",
    "Pacific/Fakaofo" -> "Pacific/Fakaofo",
    "Pacific/Enderbury" -> "Pacific/Enderbury",
    "Pacific/Efate" -> "Pacific/Efate",
    "Pacific/Easter" -> "Pacific/Easter",
    "Pacific/Chuuk" -> "Antarctica/DumontDUrville",
    "Pacific/Chatham" -> "Pacific/Chatham",
    "Pacific/Auckland" -> "Pacific/Auckland",
    "Pacific/Apia" -> "Pacific/Apia",
    "Indian/Reunion" -> "Indian/Reunion",
    "Indian/Mayotte" -> "Indian/Mayotte",
    "Indian/Mauritius" -> "Indian/Mauritius",
    "Indian/Maldives" -> "Indian/Maldives",
    "Indian/Mahe" -> "Indian/Mahe",
    "Indian/Kerguelen" -> "Indian/Kerguelen",
    "Indian/Comoro" -> "Indian/Comoro",
    "Indian/Cocos" -> "Indian/Cocos",
    "Indian/Christmas" -> "Indian/Christmas",
    "Indian/Chagos" -> "Indian/Chagos",
    "Indian/Antananarivo" -> "Indian/Antananarivo",
    "Europe/Zurich" -> "Europe/Zurich",
    "Europe/Zaporozhye" -> "Asia/Amman",
    "Europe/Zagreb" -> "Africa/Ceuta",
    "Europe/Warsaw" -> "Europe/Warsaw",
    "Europe/Volgograd" -> "Europe/Moscow",
    "Europe/Vilnius" -> "Europe/Vilnius",
    "Europe/Vienna" -> "Europe/Vienna",
    "Europe/Vatican" -> "Africa/Ceuta",
    "Europe/Vaduz" -> "Europe/Vaduz",
    "Europe/Uzhgorod" -> "Asia/Amman",
    "Europe/Tirane" -> "Europe/Tirane",
    "Europe/Tallinn" -> "Europe/Tallinn",
    "Europe/Stockholm" -> "Europe/Stockholm",
    "Europe/Sofia" -> "Europe/Sofia",
    "Europe/Skopje" -> "Africa/Ceuta",
    "Europe/Simferopol" -> "Asia/Amman",
    "Europe/Sarajevo" -> "Africa/Ceuta",
    "Europe/San_Marino" -> "Africa/Ceuta",
    "Europe/Samara" -> "Europe/Samara",
    "Europe/Rome" -> "Europe/Rome",
    "Europe/Riga" -> "Europe/Riga",
    "Europe/Prague" -> "Europe/Prague",
    "Europe/Podgorica" -> "Africa/Ceuta",
    "Europe/Paris" -> "Europe/Paris",
    "Europe/Oslo" -> "Europe/Oslo",
    "Europe/Moscow" -> "Europe/Moscow",
    "Europe/Monaco" -> "Europe/Monaco",
    "Europe/Minsk" -> "Europe/Minsk",
    "Europe/Mariehamn" -> "Asia/Amman",
    "Europe/Malta" -> "Europe/Malta",
    "Europe/Madrid" -> "Europe/Madrid",
    "Europe/Luxembourg" -> "Europe/Luxembourg",
    "Europe/London" -> "Europe/London",
    "Europe/Ljubljana" -> "Africa/Ceuta",
    "Europe/Lisbon" -> "Europe/Lisbon",
    "Europe/Kiev" -> "Europe/Kiev",
    "Europe/Kaliningrad" -> "Europe/Kaliningrad",
    "Europe/Jersey" -> "Africa/Casablanca",
    "Europe/Istanbul" -> "Europe/Istanbul",
    "Europe/Isle_of_Man" -> "Africa/Casablanca",
    "Europe/Helsinki" -> "Europe/Helsinki",
    "Europe/Guernsey" -> "Africa/Casablanca",
    "Europe/Gibraltar" -> "Europe/Gibraltar",
    "Europe/Dublin" -> "Europe/Dublin",
    "Europe/Copenhagen" -> "Europe/Copenhagen",
    "Europe/Chisinau" -> "Europe/Chisinau",
    "Europe/Budapest" -> "Europe/Budapest",
    "Europe/Bucharest" -> "Europe/Bucharest",
    "Europe/Brussels" -> "Europe/Brussels",
    "Europe/Bratislava" -> "Africa/Ceuta",
    "Europe/Berlin" -> "Europe/Berlin",
    "Europe/Belgrade" -> "Europe/Belgrade",
    "Europe/Athens" -> "Europe/Athens",
    "Europe/Andorra" -> "Europe/Andorra",
    "Europe/Amsterdam" -> "Europe/Amsterdam",
    "Australia/Sydney" -> "Australia/Sydney",
    "Australia/Perth" -> "Australia/Perth",
    "Australia/Melbourne" -> "Australia/Hobart",
    "Australia/Lindeman" -> "Antarctica/DumontDUrville",
    "Australia/Hobart" -> "Australia/Hobart",
    "Australia/Eucla" -> "Australia/Eucla",
    "Australia/Darwin" -> "Australia/Darwin",
    "Australia/Currie" -> "Australia/Hobart",
    "Australia/Broken_Hill" -> "Australia/Adelaide",
    "Australia/Brisbane" -> "Australia/Brisbane",
    "Australia/Adelaide" -> "Australia/Adelaide",
    "Atlantic/Stanley" -> "Atlantic/Stanley",
    "Atlantic/St_Helena" -> "Atlantic/St_Helena",
    "Atlantic/South_Georgia" -> "Atlantic/South_Georgia",
    "Atlantic/Reykjavik" -> "Atlantic/Reykjavik",
    "Atlantic/Madeira" -> "Africa/Casablanca",
    "Atlantic/Faroe" -> "Atlantic/Faeroe",
    "Atlantic/Cape_Verde" -> "Atlantic/Cape_Verde",
    "Atlantic/Canary" -> "Atlantic/Canary",
    "Atlantic/Bermuda" -> "Atlantic/Bermuda",
    "Atlantic/Azores" -> "Atlantic/Azores",
    "Asia/Yerevan" -> "Asia/Yerevan",
    "Asia/Yekaterinburg" -> "Asia/Yekaterinburg",
    "Asia/Yakutsk" -> "Asia/Yakutsk",
    "Asia/Vladivostok" -> "Asia/Vladivostok",
    "Asia/Vientiane" -> "Asia/Vientiane",
    "Asia/Urumqi" -> "Antarctica/Casey",
    "Asia/Ulaanbaatar" -> "Asia/Ulaanbaatar",
    "Asia/Tokyo" -> "Asia/Tokyo",
    "Asia/Thimphu" -> "Asia/Thimphu",
    "Asia/Tehran" -> "Asia/Tehran",
    "Asia/Tbilisi" -> "Asia/Tbilisi",
    "Asia/Tashkent" -> "Asia/Tashkent",
    "Asia/Taipei" -> "Asia/Taipei",
    "Asia/Singapore" -> "Asia/Singapore",
    "Asia/Shanghai" -> "Asia/Shanghai",
    "Asia/Seoul" -> "Asia/Seoul",
    "Asia/Samarkand" -> "Antarctica/Mawson",
    "Asia/Sakhalin" -> "Asia/Vladivostok",
    "Asia/Riyadh" -> "Asia/Riyadh",
    "Asia/Rangoon" -> "Asia/Rangoon",
    "Asia/Qyzylorda" -> "Antarctica/Vostok",
    "Asia/Qatar" -> "Asia/Qatar",
    "Asia/Pyongyang" -> "Asia/Pyongyang",
    "Asia/Pontianak" -> "Antarctica/Davis",
    "Asia/Phnom_Penh" -> "Asia/Phnom_Penh",
    "Asia/Oral" -> "Antarctica/Mawson",
    "Asia/Omsk" -> "Asia/Omsk",
    "Asia/Novosibirsk" -> "Asia/Omsk",
    "Asia/Novokuznetsk" -> "Asia/Omsk",
    "Asia/Nicosia" -> "Asia/Nicosia",
    "Asia/Muscat" -> "Asia/Muscat",
    "Asia/Manila" -> "Asia/Manila",
    "Asia/Makassar" -> "Asia/Makassar",
    "Asia/Magadan" -> "Asia/Magadan",
    "Asia/Macau" -> "Asia/Macau",
    "Asia/Kuwait" -> "Asia/Kuwait",
    "Asia/Kuching" -> "Antarctica/Casey",
    "Asia/Kuala_Lumpur" -> "Asia/Kuala_Lumpur",
    "Asia/Krasnoyarsk" -> "Asia/Krasnoyarsk",
    "Asia/Kolkata" -> "Asia/Colombo",
    "Asia/Kashgar" -> "Antarctica/Casey",
    "Asia/Karachi" -> "Asia/Karachi",
    "Asia/Kamchatka" -> "Asia/Kamchatka",
    "Asia/Kabul" -> "Asia/Kabul",
    "Asia/Jerusalem" -> "Asia/Jerusalem",
    "Asia/Jayapura" -> "Asia/Jayapura",
    "Asia/Jakarta" -> "Asia/Jakarta",
    "Asia/Irkutsk" -> "Asia/Irkutsk",
    "Asia/Hovd" -> "Asia/Hovd",
    "Asia/Hong_Kong" -> "Asia/Hong_Kong",
    "Asia/Ho_Chi_Minh" -> "Antarctica/Davis",
    "Asia/Harbin" -> "Antarctica/Casey",
    "Asia/Gaza" -> "Asia/Gaza",
    "Asia/Dushanbe" -> "Asia/Dushanbe",
    "Asia/Dubai" -> "Asia/Dubai",
    "Asia/Dili" -> "Asia/Dili",
    "Asia/Dhaka" -> "Asia/Dhaka",
    "Asia/Damascus" -> "Asia/Damascus",
    "Asia/Colombo" -> "Asia/Colombo",
    "Asia/Chongqing" -> "Antarctica/Casey",
    "Asia/Choibalsan" -> "Asia/Choibalsan",
    "Asia/Brunei" -> "Asia/Brunei",
    "Asia/Bishkek" -> "Asia/Bishkek",
    "Asia/Beirut" -> "Asia/Beirut",
    "Asia/Bangkok" -> "Asia/Bangkok",
    "Asia/Baku" -> "Asia/Baku",
    "Asia/Bahrain" -> "Asia/Bahrain",
    "Asia/Baghdad" -> "Asia/Baghdad",
    "Asia/Ashgabat" -> "Asia/Ashgabat",
    "Asia/Aqtobe" -> "Asia/Aqtobe",
    "Asia/Aqtau" -> "Asia/Aqtau",
    "Asia/Anadyr" -> "Asia/Kamchatka",
    "Asia/Amman" -> "Asia/Amman",
    "Asia/Almaty" -> "Asia/Almaty",
    "Asia/Aden" -> "Asia/Aden",
    "Arctic/Longyearbyen" -> "Africa/Ceuta",
    "Antarctica/Vostok" -> "Antarctica/Vostok",
    "Antarctica/Syowa" -> "Antarctica/Syowa",
    "Antarctica/South_Pole" -> "Antarctica/South_Pole",
    "Antarctica/Rothera" -> "Antarctica/Rothera",
    "Antarctica/Palmer" -> "Antarctica/Palmer",
    "Antarctica/McMurdo" -> "Antarctica/McMurdo",
    "Antarctica/Mawson" -> "Antarctica/Mawson",
    "Antarctica/Macquarie" -> "Pacific/Efate",
    "Antarctica/DumontDUrville" -> "Antarctica/DumontDUrville",
    "Antarctica/Davis" -> "Antarctica/Davis",
    "Antarctica/Casey" -> "Antarctica/Casey",
    "America/Yellowknife" -> "America/Yellowknife",
    "America/Yakutat" -> "America/Anchorage",
    "America/Winnipeg" -> "America/Winnipeg",
    "America/Whitehorse" -> "America/Whitehorse",
    "America/Vancouver" -> "America/Vancouver",
    "America/Tortola" -> "America/Tortola",
    "America/Toronto" -> "America/Toronto",
    "America/Tijuana" -> "America/Tijuana",
    "America/Thunder_Bay" -> "America/Grand_Turk",
    "America/Thule" -> "America/Thule",
    "America/Tegucigalpa" -> "America/Tegucigalpa",
    "America/Swift_Current" -> "America/Belize",
    "America/St_Vincent" -> "America/St_Vincent",
    "America/St_Thomas" -> "America/St_Thomas",
    "America/St_Lucia" -> "America/St_Lucia",
    "America/St_Kitts" -> "America/St_Kitts",
    "America/St_Johns" -> "America/St_Johns",
    "America/St_Barthelemy" -> "America/Anguilla",
    "America/Sitka" -> "America/Anchorage",
    "America/Shiprock" -> "America/Denver",
    "America/Scoresbysund" -> "America/Scoresbysund",
    "America/Sao_Paulo" -> "America/Sao_Paulo",
    "America/Santo_Domingo" -> "America/Santo_Domingo",
    "America/Santiago" -> "America/Santiago",
    "America/Santarem" -> "America/Araguaina",
    "America/Santa_Isabel" -> "America/Los_Angeles",
    "America/Rio_Branco" -> "America/Rio_Branco",
    "America/Resolute" -> "America/Chicago",
    "America/Regina" -> "America/Regina",
    "America/Recife" -> "America/Recife",
    "America/Rankin_Inlet" -> "America/Chicago",
    "America/Rainy_River" -> "America/Chicago",
    "America/Puerto_Rico" -> "America/Puerto_Rico",
    "America/Porto_Velho" -> "America/Porto_Velho",
    "America/Port-au-Prince" -> "America/Port-au-Prince",
    "America/Port_of_Spain" -> "America/Port_of_Spain",
    "America/Phoenix" -> "America/Phoenix",
    "America/Paramaribo" -> "America/Paramaribo",
    "America/Pangnirtung" -> "America/Grand_Turk",
    "America/Panama" -> "America/Panama",
    "America/Ojinaga" -> "America/Denver",
    "America/North_Dakota/New_Salem" -> "America/Chicago",
    "America/North_Dakota/Center" -> "America/Chicago",
    "America/North_Dakota/Beulah" -> "America/Chicago",
    "America/Noronha" -> "America/Noronha",
    "America/Nome" -> "America/Anchorage",
    "America/Nipigon" -> "America/Grand_Turk",
    "America/New_York" -> "America/New_York",
    "America/Nassau" -> "America/Nassau",
    "America/Montserrat" -> "America/Montserrat",
    "America/Montreal" -> "America/Montreal",
    "America/Montevideo" -> "America/Montevideo",
    "America/Monterrey" -> "America/Chicago",
    "America/Moncton" -> "America/Halifax",
    "America/Miquelon" -> "America/Miquelon",
    "America/Mexico_City" -> "America/Mexico_City",
    "America/Metlakatla" -> "Pacific/Pitcairn",
    "America/Merida" -> "America/Chicago",
    "America/Menominee" -> "America/Chicago",
    "America/Mazatlan" -> "America/Mazatlan",
    "America/Matamoros" -> "America/Chicago",
    "America/Martinique" -> "America/Martinique",
    "America/Marigot" -> "America/Anguilla",
    "America/Manaus" -> "America/Manaus",
    "America/Managua" -> "America/Managua",
    "America/Maceio" -> "America/Maceio",
    "America/Los_Angeles" -> "America/Los_Angeles",
    "America/Lima" -> "America/Lima",
    "America/La_Paz" -> "America/La_Paz",
    "America/Kentucky/Monticello" -> "America/Grand_Turk",
    "America/Kentucky/Louisville" -> "America/Grand_Turk",
    "America/Juneau" -> "America/Anchorage",
    "America/Jamaica" -> "America/Jamaica",
    "America/Iqaluit" -> "America/Iqaluit",
    "America/Inuvik" -> "America/Denver",
    "America/Indiana/Winamac" -> "America/Grand_Turk",
    "America/Indiana/Vincennes" -> "America/Grand_Turk",
    "America/Indiana/Vevay" -> "America/Grand_Turk",
    "America/Indiana/Tell_City" -> "America/Chicago",
    "America/Indiana/Petersburg" -> "America/Grand_Turk",
    "America/Indiana/Marengo" -> "America/Grand_Turk",
    "America/Indiana/Knox" -> "America/Chicago",
    "America/Indiana/Indianapolis" -> "America/Grand_Turk",
    "America/Hermosillo" -> "America/Hermosillo",
    "America/Havana" -> "America/Havana",
    "America/Halifax" -> "America/Halifax",
    "America/Guyana" -> "America/Guyana",
    "America/Guayaquil" -> "America/Guayaquil",
    "America/Guatemala" -> "America/Guatemala",
    "America/Guadeloupe" -> "America/Guadeloupe",
    "America/Grenada" -> "America/Grenada",
    "America/Grand_Turk" -> "America/Grand_Turk",
    "America/Goose_Bay" -> "America/Halifax",
    "America/Godthab" -> "America/Godthab",
    "America/Glace_Bay" -> "America/Halifax",
    "America/Fortaleza" -> "America/Fortaleza",
    "America/El_Salvador" -> "America/El_Salvador",
    "America/Eirunepe" -> "America/Anguilla",
    "America/Edmonton" -> "America/Edmonton",
    "America/Dominica" -> "America/Dominica",
    "America/Detroit" -> "America/Grand_Turk",
    "America/Denver" -> "America/Denver",
    "America/Dawson" -> "America/Los_Angeles",
    "America/Dawson_Creek" -> "America/Dawson_Creek",
    "America/Danmarkshavn" -> "America/Danmarkshavn",
    "America/Curacao" -> "America/Curacao",
    "America/Cuiaba" -> "America/Cuiaba",
    "America/Costa_Rica" -> "America/Costa_Rica",
    "America/Chihuahua" -> "America/Denver",
    "America/Chicago" -> "America/Chicago",
    "America/Cayman" -> "America/Cayman",
    "America/Cayenne" -> "America/Cayenne",
    "America/Caracas" -> "America/Caracas",
    "America/Cancun" -> "America/Chicago",
    "America/Campo_Grande" -> "America/Campo_Grande",
    "America/Cambridge_Bay" -> "America/Denver",
    "America/Boise" -> "America/Denver",
    "America/Bogota" -> "America/Bogota",
    "America/Boa_Vista" -> "America/Boa_Vista",
    "America/Blanc-Sablon" -> "America/Anguilla",
    "America/Belize" -> "America/Belize",
    "America/Belem" -> "America/Belem",
    "America/Barbados" -> "America/Barbados",
    "America/Bahia" -> "America/Bahia",
    "America/Bahia_Banderas" -> "America/Chicago",
    "America/Atikokan" -> "America/Bogota",
    "America/Asuncion" -> "America/Asuncion",
    "America/Aruba" -> "America/Aruba",
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
    "America/Argentina/Buenos_Aires" -> "America/Argentina/Buenos_Aires",
    "America/Araguaina" -> "America/Araguaina",
    "America/Antigua" -> "America/Antigua",
    "America/Anguilla" -> "America/Anguilla",
    "America/Anchorage" -> "America/Anchorage",
    "Africa/Windhoek" -> "Africa/Windhoek",
    "Africa/Tunis" -> "Africa/Tunis",
    "Africa/Tripoli" -> "Africa/Tripoli",
    "Africa/Sao_Tome" -> "Africa/Sao_Tome",
    "Africa/Porto-Novo" -> "Africa/Porto-Novo",
    "Africa/Ouagadougou" -> "Africa/Ouagadougou",
    "Africa/Nouakchott" -> "Africa/Nouakchott",
    "Africa/Niamey" -> "Africa/Niamey",
    "Africa/Ndjamena" -> "Africa/Ndjamena",
    "Africa/Nairobi" -> "Africa/Nairobi",
    "Africa/Monrovia" -> "Africa/Monrovia",
    "Africa/Mogadishu" -> "Africa/Mogadishu",
    "Africa/Mbabane" -> "Africa/Mbabane",
    "Africa/Maseru" -> "Africa/Maseru",
    "Africa/Maputo" -> "Africa/Maputo",
    "Africa/Malabo" -> "Africa/Malabo",
    "Africa/Lusaka" -> "Africa/Lusaka",
    "Africa/Lubumbashi" -> "Africa/Lubumbashi",
    "Africa/Luanda" -> "Africa/Luanda",
    "Africa/Lome" -> "Africa/Lome",
    "Africa/Libreville" -> "Africa/Libreville",
    "Africa/Lagos" -> "Africa/Lagos",
    "Africa/Kinshasa" -> "Africa/Kinshasa",
    "Africa/Kigali" -> "Africa/Kigali",
    "Africa/Khartoum" -> "Africa/Khartoum",
    "Africa/Kampala" -> "Africa/Kampala",
    "Africa/Johannesburg" -> "Africa/Johannesburg",
    "Africa/Harare" -> "Africa/Harare",
    "Africa/Gaborone" -> "Africa/Gaborone",
    "Africa/Freetown" -> "Africa/Freetown",
    "Africa/El_Aaiun" -> "Africa/El_Aaiun",
    "Africa/Douala" -> "Africa/Douala",
    "Africa/Djibouti" -> "Africa/Djibouti",
    "Africa/Dar_es_Salaam" -> "Africa/Dar_es_Salaam",
    "Africa/Dakar" -> "Africa/Dakar",
    "Africa/Conakry" -> "Africa/Conakry",
    "Africa/Ceuta" -> "Africa/Ceuta",
    "Africa/Casablanca" -> "Africa/Casablanca",
    "Africa/Cairo" -> "Africa/Cairo",
    "Africa/Bujumbura" -> "Africa/Bujumbura",
    "Africa/Brazzaville" -> "Africa/Brazzaville",
    "Africa/Blantyre" -> "Africa/Blantyre",
    "Africa/Bissau" -> "Africa/Bissau",
    "Africa/Banjul" -> "Africa/Banjul",
    "Africa/Bangui" -> "Africa/Bangui",
    "Africa/Bamako" -> "Africa/Bamako",
    "Africa/Asmara" -> "Africa/Asmera",
    "Africa/Algiers" -> "Africa/Algiers",
    "Africa/Addis_Ababa" -> "Africa/Addis_Ababa",
    "Africa/Accra" -> "Africa/Accra",
    "Africa/Abidjan" -> "Africa/Abidjan")

  override def handleTask(params: ActivityParameters) = {

    val lat = params.getRequiredParameter("lat")
    val lon = params.getRequiredParameter("lon")
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
    translator.getOrElse(zone, "America/Boise")
  }

}

object timezoneworker {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new TimeZoneWorker
      with SWFAdapterComponent with DynamoAdapterComponent {
        lazy val swfAdapter = new SWFAdapter with PropertiesLoaderComponent { lazy val config = cfg }
        lazy val dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { lazy val config = cfg }
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
