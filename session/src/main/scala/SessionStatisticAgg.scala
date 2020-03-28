import java.util.{Date, UUID}

import cn.cqjtyy.commons.conf.ConfigurationManager
import cn.cqjtyy.commons.constant.Constants
import cn.cqjtyy.commons.model._
import cn.cqjtyy.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SessionStatisticAgg {



  def main(args: Array[String]): Unit = {
    //获取查询的限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)
    //获取全局独一无二的主键
    val taskUUID = UUID.randomUUID().toString
    //创建sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("session")
    //创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //actionRDD:rdd[UserVisitAction]
    //val actionRDD = getActionRDD(sparkSession, taskParam)
    val actionRDD = getActionRDD(sparkSession,taskParam)
    //     sessionId2ActionRDD:rdd[(sid,UserVisitAction)]
    val sessionId2ActionRDD =actionRDD.map{
      item => (item.session_id,item)
    }
    // sessionId2GroupRDD :rdd[(sid,iterable(UserVisitAction))]
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    //分布
    //  sparkSession.sparkContext.setCheckpointDir()，括号中指定分布文件系统地址，以后跑集群的时候要使用。
    sessionId2GroupRDD.cache()
    //sesskonId2GropuRDD.checkpoint()
    //sessionId2GroupRDD.foreach(println(_))

    //获取聚合数据里面的聚合信息
    val sessionId2FullInfoRDD = getFullInfoData(sparkSession,sessionId2GroupRDD)

    //sessionId2FullInfoRDD.foreach(println(_))
    //创建自定义累加器
    val sessionStatAccumulator = new SessionStatAccumulator
    //注册自定义累加器
    sparkSession.sparkContext.register(sessionStatAccumulator,"sessionAccumulator")
    //第一：对数据进行过滤
    //第二：对累加器进行累加

    val sessionId2FilterRDD =  getFilteredData(taskParam,sessionStatAccumulator,sessionId2FullInfoRDD)
    //sessionId2FilterRDD.foreach(println(_))
    sessionId2FilterRDD.count()

    for((k,v) <- sessionStatAccumulator.value){
      println("k  = " + k + ", value = " +v)

    }
    //获取最终的统计结果
    getFinalData(sparkSession,taskUUID,sessionStatAccumulator.value)

    //需求二：session随机抽取
    //sessionId2FilterRDD:RDD[(sid,fullInfo)]一个session对应一条数据，也就是一个fullInfo
    sessionRandomExtract(sparkSession,taskUUID,sessionId2FilterRDD)
    //sessionId2ActioinRDD:RDD[(sessionId,action)]
    //sessionId2FilterRDD:RDD[(sessionId,FullInfo)]符合过滤条件的
    //sessionId2FilterActioinRDD：
    //获取所有符合过滤条件的action数据
    val sessionId2FilterActioinRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map{
      case (sessionId,(action,fullInfo)) =>
        (sessionId,action)
    }
    //需求三：Top10热门商品统计
    //top10CategoryArray:Array[(sortKey,countInfo)]
    val top10CategoryArray = top10PopularCategories(sparkSession,taskUUID,sessionId2FilterActioinRDD)

    //需求四：Top10热门商品的Top10活跃session统计
    //top10CategoryArray:Array[(sortKey,countInfo)]
    //sessionId2FilterActioinRDD:RDD[(sessionId,countInfo)]
    top10ActiveSession(sparkSession,taskUUID,sessionId2FilterActioinRDD,top10CategoryArray)



  }

 def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActioinRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey,String)]): Unit = {
    //第一步：过滤出所有点击过Top10品类的action
    //1：join
  /* val cid2CountInfoRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map{
 case (sortKey: SortKey,conutInfo) =>
   val cid = StringUtils.getFieldFromConcatString(conutInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
       (cid,conutInfo)

    }
   val cid2ActionRDD = sessionId2FilterActioinRDD.map{
     case (sessionId,action) =>
       val cid =action.click_category_id
       (cid,action)
   }
   val sessionId2ActionRDD = cid2CountInfoRDD.join(cid2ActionRDD).map{
     case (cid,(countInfo,action)) =>
       val sid = action.session_id
       (sid,action)
   }*/

//2.filter
   // cidArray :Array[Long]使用包含了Top10热门品类ID
   val cidArray = top10CategoryArray.map{
     case (sortKey: SortKey,countInfo) =>
       val cid =StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
       cid

   }
   //所有符合过滤条件的，并且点击过Top10热门品类的action
   val sessionId2ActionRDD = sessionId2FilterActioinRDD.filter{
     case (sessionId,action) =>
       cidArray.contains(action.click_category_id)
   }
   //按照sessionId进行聚合操作
   val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
   //cid2SessionCountRDD:RDD[(cid,sessionCount]
   val cid2SessionCountRDD = sessionId2GroupRDD.flatMap{
     case (sessionId,iterableAction)=>
     val categoryCountMap = new mutable.HashMap[Long,Long]()
     for (action <- iterableAction){
       val cid = action.click_category_id
       if(!categoryCountMap.contains(cid))
         categoryCountMap += (cid -> 0)
       categoryCountMap.update(cid,categoryCountMap(cid) + 1)

     }
     //记录了一个session对于它所有点击过的品类的点击次数
     for((cid,count) <- categoryCountMap)
       yield (cid,sessionId + "=" + count)

   }
   //cid2GroupRDD:RDD[(cid,iterableSessionCount)]
   //cid2GroupRDD每一条数据都是一个categoryid和它对应的所有点击过他的session对他的点击次数
   val cid2GroupRDD = cid2SessionCountRDD.groupByKey()

   val top10SessionRDD =cid2GroupRDD.flatMap{
     case (cid,iterableSessionCount) =>
       val sortList = iterableSessionCount.toList.sortWith((item1,item2) =>{
         item1.split("=")(1).toLong > item2.split("=")(1).toLong
       }).take(10)
       val top10Session =sortList.map{
         case item =>
           val sessionId = item.split("=")(0)
           val count = item.split("=")(1).toLong
           Top10Session(taskUUID,cid,sessionId,count)
       }
       top10Session
   }
   import  sparkSession.implicits._
   top10SessionRDD.toDF().write
     .format("jdbc")
     .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
     .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
     .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
     .option("dbtable","top10_session_0308")
     .mode(SaveMode.Append)
     .save()



 }













  def getClickCount(sessionId2FilterActioinRDD: RDD[(String, UserVisitAction)]) = {
    //val clickFilterRDD = sessonId2FilterActioniRDD.filter{
    //      case (sessionId,action) => action.click_category_id != -1L
    //}
    //先进行过滤，把点击行为对应的action保留下来
    val clickFilterRDD = sessionId2FilterActioinRDD.filter(item => item._2.click_category_id != -1L)
    //进行格式转换，以sessionId为key，action为value，转换成以click_category_id为key，1L为value，为reduceByKey做准备
    val clickNumRDD = clickFilterRDD.map{
      case (sessionId,action) => (action.click_category_id,1L)
    }
    //所有点击的次数
    clickNumRDD.reduceByKey(_+_)
  }




  def getOrderCount(sessionId2FilterActioinRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessionId2FilterActioinRDD.filter(item => item._2.order_category_ids != null)

    val orderNumRDD = orderFilterRDD.flatMap{
    //action.order_category_ids.split(","):Array[String]
    //action.orker_category_ids.split(",").map(item => (item.toLong,1L)
    //先将我们的字符串拆分成字符串数组，然后使用map转化数组中的每个元素
    //原来我们的每一个元素都是一个string，现在转化为（long，1L)
      case (sessionId,action) => action.order_category_ids.split(",")
        .map(item => (item.toLong,1L))
    }
    orderNumRDD.reduceByKey(_+_)
  }

  def getPayCount(sessionId2FilterActioinRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessionId2FilterActioinRDD.filter(item => item._2.pay_category_ids != null)
    val payNummRDD = payFilterRDD.flatMap{
      case (sid,action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong,1L))
    }
    payNummRDD.reduceByKey(_+_)


  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map{
      case(cid,(categoryId,option)) =>
        //isDefined:判断是否有数据，有数据返回数据，没有数据返回0
        val clickCount = if(option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
        Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid,aggrCount)
    }
    val cid2OderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map{
      case (cid,(clickInfo,option)) =>
        val orderCount =if(option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
        Constants.FIELD_ORDER_COUNT + "=" + orderCount
        (cid,aggrInfo)
    }
    val cid2PayInfoRDD = cid2OderInfoRDD.leftOuterJoin(cid2PayCountRDD).map{
      case (cid,(orderInfo,option)) =>
        val payCount = if(option.isDefined) option.get else 0
        val aggrInfo =orderInfo + "|" +
        Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid,aggrInfo)
    }
    cid2PayInfoRDD
  }


  //注意：这个方法要有返回值
  def top10PopularCategories(sparkSession: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActioinRDD: RDD[(String, UserVisitAction)]) = {
    //第一步：获取所有发生过的点击、下单、付款的品类

    var cid2CidRDD = sessionId2FilterActioinRDD.flatMap{
      case (sid,action) =>

        val categoryBuffer = new ArrayBuffer[(Long,Long)]()
        //点击行为
        if(action.click_category_id != -1){
          categoryBuffer += ((action.click_category_id,action.click_category_id))
          //下单行为action.order_category_ids
           }else if(action.order_category_ids != null){
          for (orderCid <- action .order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong,orderCid.toLong))
          //付款行为action.pay_category_ids
        }else if(action.pay_category_ids != null){
          for(payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong,payCid.toLong))
        }
        categoryBuffer
    }
    //去除重复记录
    cid2CidRDD = cid2CidRDD.distinct()
    //第二步：统计品类的点击次数、下单次数、付款次数
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActioinRDD)
    //cid2ClickCountRDD.foreach(println(_))
    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActioinRDD)
    val cid2PayCountRDD = getPayCount(sessionId2FilterActioinRDD)
      //cid2FullCountRDD:RDD[(cid,countInfo)]
    //(62,categoryid=62|clickCount=77|orderCount=65|payCount=67)
    val cid2FullCountRDD = getFullCount(cid2CidRDD,cid2ClickCountRDD,cid2OrderCountRDD,cid2PayCountRDD)
    //实现自定义二次排序key
    val sortKey2FullCountRDD = cid2FullCountRDD.map{
      case (cid,countInfo) =>
        val clickCount =StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount,orderCount,payCount)
        (sortKey,countInfo)
    }
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map{
      case (sortKey: SortKey,countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID,cid,clickCount,orderCount,payCount)
    }
    import  sparkSession.implicits._
    top10CategoryRDD.toDF.write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10_category_0308")
      .mode(SaveMode.Append)
      .save
    top10CategoryArray

  }




  def generateRandomIndexList(extractPerDay: Int,
                              dateSessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    for ((hour, count) <- hourCountMap) {
      //获取一个小时要抽取多少条数据
      var hourExrCount = ((count / dateSessionCount.toDouble) * extractPerDay).toInt
      //避免一个小时要抽取的数量超过这个小时的总数
      if (hourExrCount > count) {
        hourExrCount = count.toInt
      }

      val random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount) {
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }

        case Some(list) => {
          for (i <- 0 until hourExrCount) {
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        }
      }

    }
  }


  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FilterRDD: RDD[(String, String)]): Unit = {
    //dateHour2FullInfoRDD:RDD[(dateHour,fullInfo)],把sesionId为key，fullInfo为value，转换成以dateHour为key，以fullInfo为value
    val dateHour2FullInfoRDD = sessionId2FilterRDD.map{
      case (sid,fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_START_TIME)
        //dateHour:yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour,fullInfo)
    }
    //hourCountMap:Map[(dateHour,count)],按小时统计
    val hourCountMap = dateHour2FullInfoRDD.countByKey()
    //dateHourCountMap:Map[(date,Map[(hour,count)])],按天统计
    val dateHourCountMap = new mutable.HashMap[String,mutable.HashMap[String,Long]]()
    
    for((dateHour,count) <- hourCountMap){
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(0)
      
      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String,Long]()
          dateHourCountMap(date) += (hour -> count)

        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
      
    }
    //解决问题一：一共有多少天：dateHourCountMap.size
    //            一共抽取多少条：100 / dateHourCountMap.size
    val extractPerDay = 100 / dateHourCountMap.size
    //解决问题二：一天有多少session：dateHourCountMap(date).values.sum
    //解决问题三：一个小时有多少session：dateHourCountMap(date)(hour)
    val dateHourExtractIndexListMap = new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]]()
    //dateHourCountMap:Map[(date,Map[(hour,count)])]
    for((date,hourCountMap) <- dateHourCountMap){
      val dateSessionCount = hourCountMap.values.sum
      
      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String,ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay,dateSessionCount,hourCountMap,dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay,dateSessionCount,hourCountMap,dateHourExtractIndexListMap(date))
      }
      
      //到目前为止，我们获得了每个小时要抽取的session的index
      //广播大变量，提升任务性能
      val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)
      //   dateHour2FullInfoRDD:RDD[(dateHour,FullInfo)]
      //dateHour2GroupRDD:RDD[(dateHour,iterableFullInfo

      val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()
        //extractSessionRDD：RDD[SessinRandomExtract]
      val extractSessionRDD = dateHour2GroupRDD.flatMap{
        case (dateHour,iterableFullInfo) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)
          val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)
          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0
          for (fullInfo <- iterableFullInfo){
            if(extractList.contains(index)){
              val sessionId = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_SESSION_ID)
              val startTime =StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_START_TIME)
              val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategories = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS)
              val extractSession = SessionRandomExtract(taskUUID,sessionId,startTime,searchKeywords,clickCategories)
              extractSessionArrayBuffer += extractSession
            }
            index += 1
          }
          extractSessionArrayBuffer
      }
      import sparkSession.implicits._
      extractSessionRDD.toDF().write
        .format("jdcb")
        .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable","session_extract_0308")
        .mode(SaveMode.Append)
        .save()

      
      
    }
      

  }




  def getFinalData(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    //获取所有符合过滤条件的session个数
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    //不同范围访问的时长的个数
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1m_3m,0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s,0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s,0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s,0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s,0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m,0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m,0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m,0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m,0)

      //不同访问步长的session个数
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3,0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count ,2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat =SessionAggrStat(taskUUID,session_count.toInt,
      visit_length_1m_3m_ratio,
      visit_length_4s_6s_ratio,
      visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio,
        visit_length_30s_60s_ratio,
        visit_length_1m_3m_ratio,
        visit_length_3m_10m_ratio,
        visit_length_10m_30m_ratio,
        visit_length_30m_ratio,
        step_length_1_3_ratio,
        step_length_4_6_ratio,
        step_length_7_9_ratio,
        step_length_10_30_ratio,
        step_length_30_60_ratio,
        step_length_60_ratio)
    //要写入数据库,要转换成RDD,并且是Array
     val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    //写放数据库
    import sparkSession.implicits._
    statRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","session_ration_0308")
      .mode(SaveMode.Append)
      .save()
      








  }


  def calculateVisitLength(visitLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {
    if(visitLength >= 1 && visitLength <= 3){
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <=6){
      sessionStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {
    if(stepLength >=1 && stepLength <=3){
      sessionStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getFilteredData(taskParam: JSONObject,
                      sessionStatAccumulator: SessionStatAccumulator,
                      sessionId2FullInfoRDD: RDD[(String, String)])= {
    val startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS)
    //校验
    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if(endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else  " ") +
      (if(professionals !=null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "")+
      (if(cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if(sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if(keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "" )+
      (if(categoryIds !=null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else  "")
      //去除filterInfo最后的"|"
    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0,filterInfo.length - 1)

    sessionId2FullInfoRDD.filter{
      case (sessionId,fullInfo) =>
        var success =true

        if(!ValidUtils.between(fullInfo,Constants.FIELD_AGE,filterInfo,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE))
          success = false

        if(!ValidUtils.in(fullInfo,Constants.FIELD_PROFESSIONAL,filterInfo,Constants.PARAM_PROFESSIONALS))
          success = false

        if(!ValidUtils.in(fullInfo,Constants.FIELD_CITY,filterInfo,Constants.PARAM_CITIES))
          success = false

        if(!ValidUtils.equal(fullInfo,Constants.FIELD_SEX,filterInfo,Constants.PARAM_SEX))
          success = false

        if(!ValidUtils.in(fullInfo,Constants.FIELD_SEARCH_KEYWORDS,filterInfo,Constants.PARAM_KEYWORDS))
          success = false

        if(!ValidUtils.in(fullInfo,Constants.FIELD_CATEGORY_ID,filterInfo,Constants.PARAM_CATEGORY_IDS))
          success = false
        if(success){
          //只要进入此处，就代表session数据符合过滤条件，进行总数的计算
          sessionStatAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength,sessionStatAccumulator)
          calculateStepLength(stepLength,sessionStatAccumulator)
        }

        success

    }

  }




  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    var sql = "select * from user_visit_action where date >='" + startDate + "' and date <='" + endDate + "'"


    import sparkSession.implicits._
    //第一步：sparkSession.sql(sql)  ：DataFrame    这是DataFrame
    //第二步：sparkSession.sql(sql).as[UserVisitAction]  ：DateSet[UserVisitAction]  把DataFrame转换成DateSet[row]
    //第三步：sparkSession.sql(sql).as[UserVisitAction].rdd ：rdd[UserVisitAction]  把DateSet[row]转换成rdd

    //sparkSession.sql(sql).as[UserVisitAction].rdd
    sparkSession.sql(sql).as[UserVisitAction].rdd

  }

  def getFullInfoData(sparkSession: SparkSession,
                      sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map {
      case (sid, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null

        var userId = -1L

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        var stepLength = 0
        for(action: UserVisitAction <- iterableAction){
        //for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)

          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }

          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }
          val searchKeyword = action.search_keyword
          val clickCategory = action.click_category_id
          if (StringUtils.isNotEmpty(searchKeyword) &&
            !searchKeywords.toString.contains(searchKeyword))
            searchKeywords.append(searchKeyword + ",")

          if (clickCategory != -1L &&
            !clickCategories.toString.contains(clickCategory))
            clickCategories.append(clickCategory + ",")
          stepLength += 1

        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=  " + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime)
        (userId, aggrInfo)
    }
    val sql = "select * from user_info"

    import sparkSession.implicits._
    //sparkSession.sql(sql):DateFrame   DateSet[Row]
    //sparkSession.sql(sql).as[UserInfo]:DateSet[UserInfo]
    //sparkSession.sql(sql).as[UserInfo].rdd:RDD[UserInfo]
    //sparkSession.sql(sql)as[UserInfo].rdd.map( item =>(item.user_id,item)):RDD[(userId,UserInfo)]

    val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(
      item =>(item.user_id,item))

    userId2AggrInfoRDD.join(userInfoRDD).map{
      case(userId ,(aggrInfo,userInfo)) =>
        val age = userInfo.age
        val professional =userInfo.professional
        val sex =userInfo.sex
        val city =  userInfo.city

        val fullInfo = aggrInfo + "|" +
        Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL  + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city + "|"
        //从aggrInfo里面获取sessionId
        val sessionId =StringUtils.getFieldFromConcatString(aggrInfo,"\\|" ,Constants.FIELD_SESSION_ID)

        (sessionId , fullInfo)

        //学习github,在学习




    }

  }
}