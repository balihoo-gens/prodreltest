package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.AdWordsUserInterests
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

import scala.collection.mutable

class SetComparator[A](existing:Map[String,A], potential:Map[String,A]) {
  val removalCandidates = for((ekey, eitem) <- existing if !(potential contains ekey)) yield eitem
  val additionCandidates = for((pkey, pitem) <- potential if !(existing contains pkey)) yield pitem
}

abstract class AbstractAdWordsAdGroupProcessor extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
   with AdGroupCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    adGroupCreator.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(params("account"))

      val adGroup = adGroupCreator.getAdGroup(params) match {
        case group: AdGroup =>
          adGroupCreator.updateAdGroup(group, params)
        case _ =>
          adGroupCreator.createAdGroup(params)
      }

      completeTask(String.valueOf(adGroup.getId))

    } catch {
      case rateExceeded: RateExceededException =>
        // Whoops! We've hit the rate limit! Let's sleep!
        Thread.sleep(rateExceeded.error.getRetryAfterSeconds * 1200) // 120% of the the recommended wait time
        throw rateExceeded
      case exception: Exception =>
        throw exception
      case throwable: Throwable =>
        throw new Exception(throwable.getMessage)
    }
  }
}

class AdWordsAdGroupProcessor(cfg: PropertiesLoader, splogger: Splogger)
  extends AbstractAdWordsAdGroupProcessor
  with LoggingAdwordsWorkflowAdapter
  with AdGroupCreatorComponent {
    def splog = splogger

    lazy private val _swf = new SWFAdapter(cfg)
    def swfAdapter = _swf

    lazy private val _dyn = new DynamoAdapter(cfg)
    def dynamoAdapter = _dyn

    lazy private val _awa = new AdWordsAdapter(cfg)
    def adWordsAdapter = _awa

    lazy val _adGroupCreator = new AdGroupCreator(awa)
    def adGroupCreator = _adGroupCreator
}

trait AdGroupCreatorComponent {
  def adGroupCreator: AbstractAdGroupCreator with AdWordsAdapterComponent

  abstract class AbstractAdGroupCreator {
    this: AdWordsAdapterComponent
      with CampaignCreatorComponent =>

    val verticals = mutable.Map[String, Long]()

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("account", "int", "Participant AdWords account ID"),
        new ActivityParameter("name", "string", "Name of this AdGroup"),
        new ActivityParameter("campaignId", "int", "AdWords Campaign ID"),
        new ActivityParameter("bidDollars", "float", "Bid amount in dollars"),
        new ActivityParameter("status", "ENABLED|PAUSED|DELETED", ""),
        new ActivityParameter("target", "JSON", "Mysterious and magical Form Builder output!", false),
        new ActivityParameter("interests", "<interest>,<interest>,..", "List of Interests\nhttps://developers.google.com/adwords/api/docs/appendix/verticals", false),
        new ActivityParameter("exact keywords", "<keyword>,<keyword>,..", "List of EXACT Match Keywords", false),
        new ActivityParameter("broad keywords", "<keyword>,<keyword>,..", "List of BROAD Match Keywords", false),
        new ActivityParameter("phrase keywords", "<keyword>,<keyword>,..", "List of PHRASE Match Keywords", false),
        new ActivityParameter("negative keywords", "<keyword>,<keyword>,..", "List of NEGATIVE Match Keywords", false)
      ), new ActivityResult("int", "AdGroup ID"))
    }

    def getAdGroup(params: ActivityParameters): AdGroup = {

      val name = params("name")
      val campaignId = params("campaignId")
      val context = s"getAdGroup(name='$name', campaignId='$campaignId')"

      val selector = new SelectorBuilder()
        .fields("Id", "BiddingStrategyType")
        .equals("Name", name)
        .equals("CampaignId", campaignId)
        .build()

      adWordsAdapter.withErrorsHandled[AdGroup](context, {
        val page = adWordsAdapter.adGroupService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case 1 => page.getEntries(0)
          case _ => throw new Exception(s"adGroup name $name is ambiguous in campaign $campaignId")
        }
      })
    }

    /*
     * uses 'campaignCreator' from mixed in CampaignCreatorComponent
     */
    def createAdGroup(params: ActivityParameters): AdGroup = {

      val name = params("name")
      val campaignId = params("campaignId")
      val context = s"createAdGroup(name='$name', campaignId='$campaignId')"

      // We look up the campaign so we can follow it's Bidding strategy
      val campaign = campaignCreator.getCampaign(campaignId)

      val adGroup = new AdGroup()
      adGroup.setName(name)
      adGroup.setCampaignId(campaignId.toLong)
      adGroup.setStatus(AdGroupStatus.fromString(params("status")))

      val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
      val money = new Money()
      money.setMicroAmount(adWordsAdapter.dollarsToMicros(params("bidDollars").toFloat))

      // Switching on the CAMPAIGN bidding strategy
      campaign.getBiddingStrategyConfiguration.getBiddingStrategyType match {
        case BiddingStrategyType.MANUAL_CPC =>
          val bid = new CpcBid()
          bid.setBid(money)
          biddingStrategyConfiguration.setBids(Array(bid))
        case BiddingStrategyType.MANUAL_CPM =>
          val bid = new CpmBid()
          bid.setBid(money)
          biddingStrategyConfiguration.setBids(Array(bid))
        case _ =>
          throw new Exception(s"biddingStrategy ${campaign.getBiddingStrategyConfiguration.getBiddingStrategyType} is not supported!")
      }

      adGroup.setBiddingStrategyConfiguration(biddingStrategyConfiguration)

      val operation = new AdGroupOperation()
      operation.setOperand(adGroup)
      operation.setOperator(Operator.ADD)

      val newGroup = adWordsAdapter.withErrorsHandled[AdGroup](context, {
        adWordsAdapter.adGroupService.mutate(Array(operation)).getValue(0)
      })

      _processOptionalParams(newGroup, params)

      newGroup
    }

    def updateAdGroup(adGroup: AdGroup, params: ActivityParameters): AdGroup = {

      val context = s"updateAdGroup(name='${adGroup.getId}', params=$params)"

      for((param, value) <- params.params) {
        param match {
          case "status" =>
            adGroup.setStatus(AdGroupStatus.fromString(value))
          case "bidDollars" =>
            val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
            val money = new Money()
            money.setMicroAmount(adWordsAdapter.dollarsToMicros(value.toFloat))

            // Switching on the known AdGroup bidding strategy
            adGroup.getBiddingStrategyConfiguration.getBiddingStrategyType match {
              case BiddingStrategyType.MANUAL_CPC =>
                val bid = new CpcBid()
                bid.setBid(money)
                biddingStrategyConfiguration.setBids(Array(bid))
              case BiddingStrategyType.MANUAL_CPM =>
                val bid = new CpmBid()
                bid.setBid(money)
                biddingStrategyConfiguration.setBids(Array(bid))
              case _ =>
                throw new Exception(s"biddingStrategy ${adGroup.getBiddingStrategyConfiguration.getBiddingStrategyType} is not supported!")
            }

            adGroup.setBiddingStrategyConfiguration(biddingStrategyConfiguration)
          case _ =>

        }
      }

      val operation = new AdGroupOperation()
      operation.setOperand(adGroup)
      operation.setOperator(Operator.SET)

      adWordsAdapter.withErrorsHandled[AdGroup](context, {
        adWordsAdapter.adGroupService.mutate(Array(operation)).getValue(0)
      })

      _processOptionalParams(adGroup, params)

      adGroup
    }

    def _processKeywords(adGroup: AdGroup, keywords: Array[String], matchType: KeywordMatchType) = {
      val eksc = new SetComparator[Keyword](
        _comparifyKeywords(_getExistingKeywords(adGroup, matchType)),
        _comparifyKeywords(_makeNewKeywords(keywords, matchType)))

      _removeKeywords(adGroup, eksc.removalCandidates)
      _addKeywords(adGroup, eksc.additionCandidates)
    }

    def _processNegativeKeywords(adGroup: AdGroup, keywords: Array[String]) = {
      val eksc = new SetComparator[Keyword](
        _comparifyKeywords(_getExistingKeywords(adGroup, KeywordMatchType.EXACT, CriterionUse.NEGATIVE)),
        _comparifyKeywords(_makeNewKeywords(keywords, KeywordMatchType.EXACT)))

      _removeKeywords(adGroup, eksc.removalCandidates, CriterionUse.NEGATIVE)
      _addKeywords(adGroup, eksc.additionCandidates, CriterionUse.NEGATIVE)
    }

    def _addKeywords(adGroup: AdGroup, keywords: Iterable[Keyword], criterionUse:CriterionUse = CriterionUse.BIDDABLE) = {
      __mutateKeywords(adGroup, keywords, Operator.ADD, "Adding Keywords", criterionUse)
    }

    def _removeKeywords(adGroup: AdGroup, keywords: Iterable[Keyword], criterionUse:CriterionUse = CriterionUse.BIDDABLE) = {
      __mutateKeywords(adGroup, keywords, Operator.REMOVE, "Removing Keywords", criterionUse)
    }

    def __mutateKeywords(adGroup: AdGroup, keywords: Iterable[Keyword], op: Operator, context: String, criterionUse:CriterionUse = CriterionUse.BIDDABLE) = {
      val operations = new mutable.ArrayBuffer[AdGroupCriterionOperation]()

      for(keyword <- keywords) {
        val criterion = criterionUse match {
          case CriterionUse.BIDDABLE =>
            new BiddableAdGroupCriterion()
          case _ =>
            new NegativeAdGroupCriterion()
        }
        criterion.setAdGroupId(adGroup.getId)
        criterion.setCriterion(keyword)
//        println(s"Processing $context ${keyword.getText}")

        val operation = new AdGroupCriterionOperation()
        operation.setOperand(criterion)
        operation.setOperator(op)

        operations += operation
      }

      if(operations.length > 0) {
          adWordsAdapter.withErrorsHandled[Any](context, {
            adWordsAdapter.adGroupCriterionService.mutate(operations.toArray)
          })
      }
    }

    def _makeNewKeywords(keywords: Iterable[String], matchType: KeywordMatchType): Array[Keyword] = {
      val nkeywords = new mutable.MutableList[Keyword]()
      for(keyword <- keywords) {
        val nkeyword = new Keyword()
        nkeyword.setText(keyword)
        nkeyword.setMatchType(matchType)
        nkeywords += nkeyword
      }
      nkeywords.toArray
    }

    def _getExistingKeywords(adGroup: AdGroup, matchType: KeywordMatchType, criterionUse: CriterionUse = CriterionUse.BIDDABLE): Array[Keyword] = {

      val existingSelector = new SelectorBuilder()
        .fields("Id", "KeywordText")
        .equals("AdGroupId", String.valueOf(adGroup.getId))
        .equals("CriteriaType", "KEYWORD")
        .equals("KeywordMatchType", matchType.getValue)
        .equals("CriterionUse", criterionUse.getValue)
        .build()

      val existing: AdGroupCriterionPage = adWordsAdapter.adGroupCriterionService.get(existingSelector)
      if(existing.getTotalNumEntries == 0) {
        return Array[Keyword]()
      }

      for(page <- existing.getEntries) yield page.getCriterion.asInstanceOf[Keyword]
    }

    def _comparifyKeywords(keywords: Iterable[Keyword]): Map[String, Keyword] = {
      (for(keyword <- keywords) yield s"${keyword.getMatchType.toString}${keyword.getText}" -> keyword).toMap
    }

    def _processInterests(adGroup: AdGroup, interests: Array[String]) = {
      val eisc = new SetComparator[CriterionUserInterest](
        _comparifyInterests(_getExistingInterests(adGroup)),
        _comparifyInterests(_makeNewInterests(interests)))

      _removeInterests(adGroup, eisc.removalCandidates)
      _addInterests(adGroup, eisc.additionCandidates)
    }

    /**
     * Hit the API to get the verticals (would happen only once per running instance)
     * We currently don't need to do this because AdWordsUserInterests has this information
     * hard coded.
     */
    def __fetchVerticals() = {

      if(verticals.size == 0) {
        for(vertical <- adWordsAdapter.constantDataService.getVerticalCriterion) {
          verticals(vertical.getPath.last) = vertical.getVerticalId
        }
      }
    }

    def _makeNewInterests(interests: Iterable[String]): Array[CriterionUserInterest] = {

//      __fetchVerticals() // Some time in the future this may make more sense. For now we'll use the cached stuff.

      val ninterests = new mutable.MutableList[CriterionUserInterest]()
      for(interest <- interests) {
        val ninterest = new CriterionUserInterest()
        ninterest.setUserInterestId(AdWordsUserInterests.getInterestId(interest))
        ninterest.setUserInterestName(interest)
        ninterests += ninterest
      }
      ninterests.toArray
    }

    def _getExistingInterests(adGroup: AdGroup): Array[CriterionUserInterest] = {

      val existingSelector = new SelectorBuilder()
        .fields("UserInterestId", "UserInterestName")
        .equals("AdGroupId", String.valueOf(adGroup.getId))
        .equals("CriteriaType", "USER_INTEREST")
        .build()

      val existing: AdGroupCriterionPage = adWordsAdapter.adGroupCriterionService.get(existingSelector)
      if(existing.getTotalNumEntries == 0) {
        return Array[CriterionUserInterest]()
      }

      for(page <- existing.getEntries) yield page.getCriterion.asInstanceOf[CriterionUserInterest]
    }

    def _comparifyInterests(interests: Iterable[CriterionUserInterest]): Map[String, CriterionUserInterest] = {
      (for(interest <- interests) yield interest.getUserInterestName -> interest).toMap
    }

    def _addInterests(adGroup: AdGroup, interests: Iterable[CriterionUserInterest]) = {
      __mutateInterests(adGroup, interests, Operator.ADD, "Adding Interests")
    }

    def _removeInterests(adGroup: AdGroup, interests: Iterable[CriterionUserInterest]) = {
      __mutateInterests(adGroup, interests, Operator.REMOVE, "Removing Interests")
    }

    def __mutateInterests(adGroup: AdGroup, interests: Iterable[CriterionUserInterest], op: Operator, context: String) = {
      val operations = new mutable.ArrayBuffer[AdGroupCriterionOperation]()

      for(interest <- interests) {
        val criterion = new BiddableAdGroupCriterion()
        criterion.setAdGroupId(adGroup.getId)
        criterion.setCriterion(interest)
//        println(s"Processing $context ${interest.getUserInterestName} ${interest.getUserInterestId}")

        val operation = new AdGroupCriterionOperation()
        operation.setOperand(criterion)
        operation.setOperator(op)

        operations += operation
      }

      if(operations.length > 0) {
        adWordsAdapter.withErrorsHandled[Any](context, {
          adWordsAdapter.adGroupCriterionService.mutate(operations.toArray)
        })
      }
    }

    /**
     * This function is the unfortunate collision of not enough planning and stuff.
     * TODO Compell them to stop using this.
     * @param adGroup AdGroup
     * @param targetJson String JSON straight from the bowls of formbuilder, not an ideal format.
     * @return
     */
    def _processTargetingDeprecatedYucky(adGroup: AdGroup, targetJson: String) = {
      val target = Json.parse(targetJson).as[JsObject]
      val focus = target.value("focus").as[JsString].value

      focus match {
        case "interests" =>
          val interests = target.value("interests").as[JsArray]
          _processInterests(adGroup, for(i <- interests.value.toArray) yield i.as[String])
        case "keywords" =>
          val keywords = target.value("keywords").as[JsString]
          _processKeywords(adGroup, for(s <- keywords.value.split(",")) yield s.trim, KeywordMatchType.BROAD)
        case _ =>
      }
    }

    def _processOptionalParams(adGroup: AdGroup, params: ActivityParameters) = {

      for((param, value) <- params.params) {
        param match {
          case "target" => // This one is dumb and needs to go!
            _processTargetingDeprecatedYucky(adGroup, value)
          case "interests" =>
            _processInterests(adGroup, value.split(","))
          case "exact keywords" =>
            _processKeywords(adGroup, value.split(","), KeywordMatchType.EXACT)
          case "broad keywords" =>
            _processKeywords(adGroup, value.split(","), KeywordMatchType.BROAD)
          case "phrase keywords" =>
            _processKeywords(adGroup, value.split(","), KeywordMatchType.PHRASE)
          case "negative keywords" =>
            _processNegativeKeywords(adGroup, value.split(","))
          case _ =>
        }
      }
    }

  }

  class AdGroupCreator(awa: AdWordsAdapter)
    extends AbstractAdGroupCreator
    with CampaignCreatorComponent
    with AdWordsAdapterComponent {
      def adWordsAdapter = awa
      private val _campaignCreator = new CampaignCreator(adWordsAdapter)
      //this is a getter method, not a creator, so no new in here.
      def campaignCreator = _campaignCreator
  }
}


object adwords_adgroupprocessor {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    splog("INFO", s"Started $name")
    try {
      val cfg = PropertiesLoader(args, name)
      val worker = new AdWordsAdGroupProcessor(cfg, splog)
      worker.work()
    }
    catch {
      case t:Throwable =>
        splog("ERROR", t.getMessage)
    }
    splog("INFO", s"Terminated $name")
  }
}


