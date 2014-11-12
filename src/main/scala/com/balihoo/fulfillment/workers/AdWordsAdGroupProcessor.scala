package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.AdWordsUserInterests
import com.balihoo.fulfillment.util.Splogger

import com.google.api.ads.adwords.axis.utils.v201406.SelectorBuilder
import com.google.api.ads.adwords.axis.v201406.cm._
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
    adWordsAdapter.withErrorsHandled[Any]("AdGroup Processor", {
      adWordsAdapter.setClientId(params("account"))

      val adGroup = adGroupCreator.getAdGroup(params) match {
        case group: AdGroup =>
          adGroupCreator.updateAdGroup(group, params)
        case _ =>
          adGroupCreator.createAdGroup(params)
      }

      completeTask(String.valueOf(adGroup.getId))
    })
  }
}

class AdWordsAdGroupProcessor(override val _cfg: PropertiesLoader, override val _splog: Splogger)
extends AbstractAdWordsAdGroupProcessor
  with LoggingAdwordsWorkflowAdapterImpl
  with AdGroupCreatorComponent {
    lazy val _adGroupCreator = new AdGroupCreator(adWordsAdapter)
    def adGroupCreator = _adGroupCreator
}

trait AdGroupCreatorComponent {
  def adGroupCreator: AbstractAdGroupCreator with AdWordsAdapterComponent

  // https://developers.google.com/adwords/api/docs/appendix/platforms
  val PLATFORM_DESKTOP:Long = 30000
  val PLATFORM_MOBILE:Long = 30001
  val PLATFORM_TABLET:Long = 30002

  abstract class AbstractAdGroupCreator {
    this: AdWordsAdapterComponent
      with CampaignCreatorComponent =>

    val verticals = mutable.Map[String, Long]()

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new StringActivityParameter("account", "Participant AdWords account ID"),
        new StringActivityParameter("name", "Name of this AdGroup"),
        new StringActivityParameter("campaignId", "AdWords Campaign ID"),
        new NumberActivityParameter("bidDollars", "Bid amount in dollars"),
        new NumberActivityParameter("mobile bid modifier", "Proportion of bidDollars to be bid for mobile", false),
        new EnumActivityParameter("status", "", List("ENABLED","PAUSED","DELETED")),
        new ObjectActivityParameter("target", /*"JSON",*/ "Mysterious and magical Form Builder output!", false),
        new StringsActivityParameter("interests", "List of Interests\nhttps://developers.google.com/adwords/api/docs/appendix/verticals", false),
        new StringsActivityParameter("exact keywords", "List of EXACT Match Keywords", false),
        new StringsActivityParameter("broad keywords", "List of BROAD Match Keywords", false),
        new StringsActivityParameter("phrase keywords", "List of PHRASE Match Keywords", false),
        new StringsActivityParameter("negative keywords", "List of NEGATIVE Match Keywords", false)
      ), new ActivityResult("int", "AdGroup ID"))
    }

    def getAdGroup(params:String):AdGroup = {
      getAdGroup(Json.parse(params).as[JsObject])
    }

    def getAdGroup(params:JsObject):AdGroup = {
      getAdGroup(getSpecification.getParameters(params))
    }

    def getAdGroup(params: ActivityParameters): AdGroup = {

      val name = params[String]("name")
      val campaignId = params[String]("campaignId")
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

    def createAdGroup(params:String):AdGroup = {
      createAdGroup(Json.parse(params).as[JsObject])
    }

    def createAdGroup(params:JsObject):AdGroup = {
      createAdGroup(getSpecification.getParameters(params))
    }

    /*
     * uses 'campaignCreator' from mixed in CampaignCreatorComponent
     */
    def createAdGroup(params: ActivityParameters): AdGroup = {

      val name = params[String]("name")
      val campaignId = params[String]("campaignId")
      val context = s"createAdGroup(name='$name', campaignId='$campaignId')"

      // We look up the campaign so we can follow it's Bidding strategy
      val campaign = campaignCreator.lookupCampaign(campaignId)

      val adGroup = new AdGroup()
      adGroup.setName(name)
      adGroup.setCampaignId(campaignId.toLong)
      adGroup.setStatus(AdGroupStatus.fromString(params[String]("status")))

      val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
      val money = new Money()
      money.setMicroAmount(adWordsAdapter.dollarsToMicros(params[Float]("bidDollars")))

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

    def updateAdGroup(adGroup: AdGroup, params:String):AdGroup = {
      updateAdGroup(adGroup, Json.parse(params).as[JsObject])
    }

    def updateAdGroup(adGroup: AdGroup, params:JsObject):AdGroup = {
      updateAdGroup(adGroup, getSpecification.getParameters(params))
    }

    def updateAdGroup(adGroup: AdGroup, params: ActivityParameters): AdGroup = {

      val context = s"updateAdGroup(name='${adGroup.getId}', params=$params)"

      if(params.has("status")) {
        adGroup.setStatus(AdGroupStatus.fromString(params[String]("status")))
      }

      if(params.has("bidDollars")) {
        val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
        val money = new Money()
        money.setMicroAmount(adWordsAdapter.dollarsToMicros(params[Float]("bidDollars")))

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

    def _processKeywords(adGroup: AdGroup, keywords: Seq[String], matchType: KeywordMatchType) = {
      val eksc = new SetComparator[Keyword](
        _comparifyKeywords(_getExistingKeywords(adGroup, matchType)),
        _comparifyKeywords(_makeNewKeywords(keywords, matchType)))

      _removeKeywords(adGroup, eksc.removalCandidates)
      _addKeywords(adGroup, eksc.additionCandidates)
    }

    def _processNegativeKeywords(adGroup: AdGroup, keywords: Seq[String]) = {
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
        nkeyword.setText(AdWordsPolicy.validateKeyword(keyword))
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

    def _processInterests(adGroup: AdGroup, interests: Seq[String]) = {
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

    def _getExistingBidModifier(adGroup: AdGroup, platformId:Long): Option[AdGroupBidModifier] = {

      val existingSelector = new SelectorBuilder()
        .fields("AdGroupId", "BidModifier", "Id")
        .equals("AdGroupId", String.valueOf(adGroup.getId))
        .equals("Id", String.valueOf(platformId))
        .build()

      try {
        val existing: AdGroupBidModifierPage = adWordsAdapter.adGroupBidModifierService.get(existingSelector)
        if(existing.getTotalNumEntries == 0) {
          return None
        } else if(existing.getEntries(0).getBidModifier == null) {
          return None
        }

        Some(existing.getEntries(0))
      } catch {
        case e:Exception =>
          if(e.getMessage.contains("INVALID_ID")) {
            None
          } else {
            throw e
          }
      }
    }

    def _processBidModifier(adGroup:AdGroup, modifier:String, platformId:Long) = {

      val operations = new mutable.ArrayBuffer[AdGroupBidModifierOperation]()

      val platform = new Platform()
      platform.setId(platformId)

      var operator:Operator = Operator.SET
      val bidModifier =  _getExistingBidModifier(adGroup, platformId) match {
        case m:Some[AdGroupBidModifier] =>
          m.get
        case _ =>
          operator = Operator.ADD
          new AdGroupBidModifier()
      }

      bidModifier.setAdGroupId(adGroup.getId)
      bidModifier.setBidModifier(modifier.toDouble)
      bidModifier.setCriterion(platform)

      val operation = new AdGroupBidModifierOperation()
      operation.setOperand(bidModifier)
      operation.setOperator(operator)

      operations += operation

      adWordsAdapter.withErrorsHandled[Any](s"${operator.getValue} bid modifier $modifier", {
        adWordsAdapter.adGroupBidModifierService.mutate(operations.toArray)
      })
    }

    /**
     * This function is the unfortunate collision of not enough planning and stuff.
     * TODO Compell them to stop using this.
     * @param adGroup AdGroup
     * @param targetJson String JSON straight from the bowels of formbuilder, not an ideal format.
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

      if(params.has("interests")) {
        _processInterests(adGroup, params[List[String]]("interests"))
      }

      if(params.has("exact keywords")) {
        _processKeywords(adGroup, params[List[String]]("exact keywords"), KeywordMatchType.EXACT)
      }

      if(params.has("broad keywords")) {
        _processKeywords(adGroup,params[List[String]]("broad keywords"), KeywordMatchType.BROAD)
      }

      if(params.has("phrase keywords")) {
        _processKeywords(adGroup, params[List[String]]("phrase keywords"), KeywordMatchType.PHRASE)
      }

      if(params.has("negative keywords")) {
        _processNegativeKeywords(adGroup, params[List[String]]("negative keywords"))
      }

      if(params.has("mobile bid modifier")) {
        _processBidModifier(adGroup, params[String]("mobile bid modifier"), PLATFORM_MOBILE)
      }

      if(params.has("target")) {  // TODO FIXME This one is dumb and needs to go!
        _processTargetingDeprecatedYucky(adGroup, params[String]("target"))
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

object adwords_adgroupprocessor extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new AdWordsAdGroupProcessor(cfg, splog)
  }
}

