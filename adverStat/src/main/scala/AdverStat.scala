import java.util.Date

import cn.cqjtyy.commons.conf.ConfigurationManager
import cn.cqjtyy.commons.constant.Constants
import cn.cqjtyy.commons.utils.DateUtils
import cn.cqjtyy.stream._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer




object AdverStat {




  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")


    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
    //val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir,func)这个是比较规范的写法
    val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics =ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)


    val kafkaParam = Map(
      "boostrap.servers" -> kafka_brokers,
      "key.deserailizer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      //auto.offset.reset
      //latest:先去zookerper获取offset，如果有，直接使用，如果 没有，从最新的数据开始消费
      //earlist:先去ziikerper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费
      //none：先去zookeeper获取offset，如果 有，直接使用，如果没有，直接报错

      "auto.offset.reset" ->"latest",
      "enable.auto.commit" ->(false:java.lang.Boolean)

    )
    val adRealTimeDStream = KafkaUtils.createDirectStream[String,String](streamingContext,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(kafka_topics),kafkaParam)

    )
    //取出了DStream里面每一条数据的value值
    //adReadTimeValueDStream:DSteam[RDD RDD RDD ...]   RDD[String]
    //String:timestamp province city userid adid
    val adReadTimeValueDStream = adRealTimeDStream.map(item =>item.value())
    //transform 遍历logRDD里面的每个RDD
    //不是黑名单用户的实时数据
    val adRealTimeFilterDStream = adReadTimeValueDStream.transform{
      logRDD =>
        //blackListArray:Array[AdBlacklist]    AdBlacklist:useId
        //blackListArray:获取到黑名单信息
        val blackListArray = AdBlacklistDAO.findAll()

        //userIdArray:Array[Long] [userId1,userId2,userId3,...]取出userId
      val userIdArray = blackListArray.map(item => item.userid)
      //过滤掉黑名单的人
      logRDD.filter{
        //log:timestamp province city userid adid
        case log =>
          val logSplit =log.split(" ")
          val useId = logSplit(3).toLong
          !userIdArray.contains(useId)
      }
    }
    //反序列化操作
    //要使用updateStateByKey算子，必须指定checkpoint目录
    streamingContext.checkpoint("./spark-streaming")

    //checkpoint(Duration(10000)):这个时间间隔必须是创建streamingContext时间的整数倍
    adRealTimeFilterDStream.checkpoint(Duration(10000))

    //需求一：实时维护黑名单
    generateBlackList(adRealTimeFilterDStream)

    //需求二：各省各城市一天中的广告点击量（累积统计）
    val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

    //需求三：统计各省Top3热门广告
    provinceTop3Adver(sparkSession,key2ProvinceCityCountDStream)

    //需求四：最近一个小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)

    //adRealTimeFilterDStream.foreachRDD(rdd =>rdd.foreach(println(_)))  打印
    streamingContext.start()
    streamingContext.awaitTermination()

  }
  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map{
      //log:timestamp province city userId adid
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toString
        //yyMMddHHmm
      val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adid = logSplit(4).toLong
        val key = timeMinute + "_" + adid
        (key,1L)
    }
    //reduceByKeyAndWindow():计算窗口函数，1小时滑动窗口内的广告点击趋势

    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a:Long,b:Long) =>(a+b),Minutes(60),Minutes(1))
    key2WindowDStream.foreachRDD{
      //(key,count)
      items =>
        val trendArray = new ArrayBuffer[AdClickTrend]()
        for((key,count) <- items){
          val keySplit = key.split("_")
          //yyyyMMddHHmm
          val timeMinute = keySplit(0)
          val date = timeMinute.substring(0,8)
          val hour = timeMinute.substring(8,10)
          val minute = timeMinute.substring(10)
          val adid = keySplit(1).toLong

          trendArray += AdClickTrend(date,hour,minute,adid,count)
        }
        AdClickTrendDAO.updateBatch(trendArray.toArray)
    }
  }

  def provinceTop3Adver(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    //key2ProvinceCityCountDStream：[RDD[key，count]]
    //key:date_province_city_adid
    //key2ProvinceCountDStream:[RDD[(newkey,count)]]
    //newKey:date_province_adid
    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map{
      case (key,count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid =  keySplit(3)

        val newKey = date + "_" + province + "_" + adid
        (newKey,count)
    }
    //key2ProvinceAggrCountDStream:把date_province_city_adid中city去掉后，date_province_adid有多条记录，按省份进行聚合
    val key2ProvinceAggrCountDStream =key2ProvinceCountDStream.reduceByKey(_+_)
      //transform：拿到每一个RDD
    val top3DStream = key2ProvinceAggrCountDStream.transform{

      rdd =>
        //rdd:RDD[(key,count)]
        //key:date_province_adid
        //把RDD转换成四个元素，每个无素是一个table
        val basicDateRDD = rdd.map{
          case (key,count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong
            (date,province,adid,count)

        }
        import sparkSession.implicits._
        // basicDateRDD.toDF转换成DF
        //basicDateRDD.toDF("date","province","adid","count").createOrReplaceTempView("tmp_basic_info")转换成一张表
        basicDateRDD.toDF("date","province","adid","count").createOrReplaceTempView("tmp_basic_info")
        val sql = "select date,province,adid,count from(" +
        "select date,province,adid,count," +
        //partition by date,province 按date和province进行分组
        //desc:倒序
          "row_number() over(partition by date,province order by count desc) rank from tmp_basic_info) t "  +
        "where rank <=3"
        sparkSession.sql(sql).rdd
    }

    top3DStream.foreachRDD{
      //rdd:RDD[row]
      rdd =>
        //foreachPartition 循环分区,拿到每个分区的数据
        rdd.foreachPartition{
          //items:row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for(item <- items){
              //从row中提取每个字段
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date,province,adid,count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }
  }

  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    //adRealTimeFilterDStream:DStream[RDD[String]] String -> log:timestamp province city usrid adid
    //key2ProvinceCityDStream:DStream[RDD[key,1L]]

    val key2ProvinceCityDStream = adRealTimeFilterDStream.map{
      case log =>
        val logSplit =log.split(" ")
        val timeStamp = logSplit(0).toLong

        //dateKey:yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province =logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4)

        val key = dateKey + "_" + province +"_" + city +"_" + adid
        (key,1L)
    }
    //key2StateDStream:某一天一个省的一个城市中某一个广告的点击次数（累计）
    //updateStateByKey全局累加值
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long]{
      (values:Seq[Long],state:Option[Long]) =>
        var newValue = 0L
        if(state.isDefined)
          newValue = state.get
        for(value <- values){
          newValue += value
        }
        Some(newValue)
    }
    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray =new ArrayBuffer[AdStat]()
          //key:date province city adid
          for ((key,count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city =keySplit(2)
            val adid = keySplit(3).toLong

            adStatArray +=AdStat(date,province,city,adid,count)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }
    key2StateDStream
  }

  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    //adRealTimeFilterDStream:DStream[RDD[String]]   String -> log:timestamp province city userid adid
    //key2NumDStream:  [RDD[(key,1L)]]
    val key2NumDStream = adRealTimeFilterDStream.map{
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        //yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adid = logSplit(4).toLong
        val key = dateKey + "_" + userId + "_" + adid
        (key,1L)
    }

    val key2CountDStream = key2NumDStream.reduceByKey(_+_)

    //根据每一个RDD里面的数据，更新用户点击次数表
    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for((key,count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adid = keySplit(2).toLong
            clickCountArray += AdUserClickCount(date,userId,adid,count)

          }

          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }
    //key2BlackListDStream:DStream[RDD(key,count)]]
    val key2BlackListDStream = key2CountDStream.filter{
      case (key,count) =>
        val keySplit = key.split("_")
        val date =keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date,userId,adid)
        if(clickCount > 100){
          true
        }else{
          false
        }
    }
    //key2BlackListDStream.map:DStream[RDD[userId]]
    val userIdDStream = key2BlackListDStream.map{
      case (key,count) => key.split("_")(1).toLong
        //distinct():去除重复的userId
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()

          for(userId <- items){
          userIdArray += AdBlacklist(userId)
        }
        AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }
  }

}
