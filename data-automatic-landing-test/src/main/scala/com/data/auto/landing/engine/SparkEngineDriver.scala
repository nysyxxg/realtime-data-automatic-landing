package com.data.auto.landing.engine

import java.io.File
import java.net.URI
import java.util.Properties

import com.data.auto.landing.common.util.JsonUtils
import com.data.auto.landing.output.hive.LandingToHive
import com.data.auto.landing.output.mysql.LandToMySQL
import com.data.auto.landing.engine.util.KafkaOffsetUtil
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.commons.cli.{HelpFormatter, Options, PosixParser}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext, StreamingContextState}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source
import com.data.auto.landing.common.jdk7.ScalaAutoCloseable.wrapAsScalaAutoCloseable
import com.data.auto.landing.output.hbase.LandToHbase
import com.data.auto.landing.output.log.LandToLogInfo
import com.data.auto.landing.parsing.jsondata.ReaderJsonData
import com.data.auto.landing.table.metadata.store.MetaDataStore

import scala.collection.mutable.ListBuffer

object SparkEngineDriver {

  private val BOOTSTRAP_SERVER = ("bootstrapServers", s"${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG},半角逗号【,】隔开")

  private val KAFKA_GROUP_ID = ("groupId", ConsumerConfig.GROUP_ID_CONFIG)

  private val KAFKA_AUTO_OFFSET = ("reset", s"${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}的值,只能是:[latest, earliest]中的一个,默认是latest")

  private val KAFKA_TOPIC = ("kafkaTopic", "kafkaTopic,多个topic用半角逗号【,】隔开")

  private val ERROR_DATE_PATH = ("errorDataPath", "错误数据输出的目录")

  private val CHECKPOINT_PATH = ("checkPointPath", "检查点数据输出的目录")

  private val PROCESS_FILE = ("processFile", "hdfs上的进程文件,删除即停止.")

  private val HIVE_DB_NAME = ("hiveDbName", "存储数据库名")

  private val HIVE_FILTER_TABLS = ("hiveFilterTables", "只写入对应的hive表");

  private val RUN_PARAM = List(BOOTSTRAP_SERVER, KAFKA_GROUP_ID, KAFKA_AUTO_OFFSET,
    KAFKA_TOPIC, HIVE_DB_NAME, ERROR_DATE_PATH, CHECKPOINT_PATH, PROCESS_FILE, HIVE_FILTER_TABLS)

  private val SPARK_CMD = "spark-submit "

  private val log = LoggerFactory.getLogger(getClass)

  def getSparkDstramByOffset(streamingContext: StreamingContext, kafkaTopic: Array[String], kafkaParams: Map[String, Object],
                             offsetMap: mutable.Map[TopicPartition, Long]) = {
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.size > 0) {
      //有记录offset，从该offset处开始消费
      KafkaUtils.createDirectStream[String, String](streamingContext,
        LocationStrategies.PreferConsistent, //位置策略：该策略,会让Spark的Executor和Kafka的Broker均匀对应
        ConsumerStrategies.Subscribe[String, String](kafkaTopic, kafkaParams, offsetMap)) //消费策略
    } else {
      //MySQL中没有记录offset,则直接连接,从latest开始消费
      KafkaUtils.createDirectStream[String, String](streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](kafkaTopic, kafkaParams))
    }
    recordDStream
  }

  def processEveryLine(key: String, records: Iterable[Seq[(String, Map[String, String])]], spark: SparkSession,
                       hiveDbName: String, groupId: String, dbConfigfile: File, hiveFilterTables: Set[String]) = {
    var keyArray = key.split("_")
    var dbType = keyArray(0)
    var database = keyArray(1)
    //判断数据库是否存在
    var output = if ("hive".equalsIgnoreCase(dbType)) {
      log.info(s"hive库名是【$database】,数据存入Hive.")
      new LandingToHive(groupId, hiveDbName, hiveFilterTables)
    } else if ("mysql".equalsIgnoreCase(dbType)) {
      log.info(s"数据开始写入.....Mysql............................." + database)
      new LandToMySQL(groupId, dbConfigfile)
    } else if ("hbase".equalsIgnoreCase(dbType)) {
      log.info(s"数据开始写入.....Hbase............................." + database)
      new LandToHbase(groupId, dbConfigfile)
    } else {
      log.info(s"数据开始写入.....日志文件............................." + database)
      new LandToLogInfo(dbConfigfile)
    }
    records.foreach(list => {
      if (!list.isEmpty) {
        output.writeRDD(list, spark, hiveFilterTables)
      }
    })
  }


  def processJsonData(value: ConsumerRecord[String, String]): Tuple2[String, Seq[(String, Map[String, String])]] = {
    var jsonMap: Map[String, Any] = JsonUtils.toObject(value.value, new TypeReference[Map[String, Any]] {})
    var dbType = jsonMap.getOrElse("dbType", "").asInstanceOf[String]
    var database = jsonMap.getOrElse("database", "").asInstanceOf[String]
    // 处理每条数据
    var data = try {
      ReaderJsonData.readRecord(jsonMap)
    } catch {
      case _: Exception =>
        log.info("错误数据，无法解析--->" + value.value)
        Seq.empty
    }
    (dbType + "_" + database, data)
  }

  def main(args: Array[String]) {

    val options = new Options
    // 初始化参数
    RUN_PARAM.foreach {
      p => options.addOption(p._1.substring(0, 1), p._1, true, p._2)
    }

    val jarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    val helpFormatter = new HelpFormatter
    helpFormatter.setWidth(Short.MaxValue.toInt)
    helpFormatter.printHelp(SPARK_CMD + jarPath, options)

    // 读取参数
    val commandLine = new PosixParser().parse(options, args, true)

    // 输出参数到屏幕
    RUN_PARAM.filter {
      tuple => commandLine.hasOption(tuple._1)
    }.foreach {
      tuple => log.info(s"${tuple._2}(${tuple._1}):${commandLine.getOptionValue(tuple._1)}")
    }

    val kafkaTopic = StringUtils.split(commandLine.getOptionValue(KAFKA_TOPIC._1), ',')

    if (kafkaTopic == null || kafkaTopic.isEmpty) {
      log.error(s"${KAFKA_TOPIC._2} ${StringUtils.join(kafkaTopic, ',')} 错误.")
      return
    }

    val bootstrapServers = commandLine.getOptionValue(BOOTSTRAP_SERVER._1)
    if (StringUtils.isBlank(bootstrapServers)) {
      log.error(s"${BOOTSTRAP_SERVER._2} $bootstrapServers 错误.")
      return
    }

    val groupId = commandLine.getOptionValue(KAFKA_GROUP_ID._1)
    if (StringUtils.isBlank(groupId)) {
      log.error(s"${KAFKA_GROUP_ID._2} $groupId 错误.")
      return
    }

    val offsetReset = commandLine.getOptionValue(KAFKA_AUTO_OFFSET._1, "latest")

    val processFile = commandLine.getOptionValue(PROCESS_FILE._1)
    if (StringUtils.isBlank(processFile)) {
      log.error(s"${PROCESS_FILE._2} $processFile 错误.")
      return
    }
    val processFilePath = new Path(processFile)

    val errorDataPath = commandLine.getOptionValue(ERROR_DATE_PATH._1)

    val checkpointDataPath = commandLine.getOptionValue(CHECKPOINT_PATH._1)

    val hiveDbName = commandLine.getOptionValue(HIVE_DB_NAME._1)

    val hiveFilterTables = StringUtils.split(commandLine.getOptionValue(HIVE_FILTER_TABLS._1, StringUtils.EMPTY), ',').toSet

    val appName = s"Ods Loader: Kafka [ ${kafkaTopic.mkString(" ")} ] to ${if (StringUtils.isBlank(hiveDbName)) "MySQL" else hiveDbName}"

    val spark = SparkSession.builder.appName(appName).master("local[2]").enableHiveSupport.getOrCreate

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fileSystem.create(processFilePath).use(_ => Unit)

    val sparkConf = spark.sparkContext.getConf
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.streaming.backpressure.initialRate", "1000")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
    sparkConf.set("spark.files", "D://data-atuo-landing//realtime-data-automatic-landing//data-automatic-landing-test//src//main//resources//database.properties");

    val dbConfigfile = getPropFileFromSparkConf(sparkConf, "database.properties")
    val properties = getProperties(dbConfigfile)
    val connectInfo = (properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password"))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    log.info(JsonUtils.toJson(kafkaParams))
    val offsetMap: mutable.Map[TopicPartition, Long] = KafkaOffsetUtil.getOffsetMap(connectInfo._1, connectInfo._2, connectInfo._3, groupId, kafkaTopic(0))

    val streamingContext = new StreamingContext(spark.sparkContext, Minutes(1))
    if (StringUtils.isNotBlank(checkpointDataPath)) {
      streamingContext.checkpoint(checkpointDataPath)
    }

    val connectInfoBC = spark.sparkContext.broadcast(connectInfo)
    val groupIdBC = spark.sparkContext.broadcast(groupId)
    val dStream = getSparkDstramByOffset(streamingContext, kafkaTopic, kafkaParams, offsetMap)

    if (StringUtils.isNotBlank(checkpointDataPath)) {
      dStream.checkpoint(Minutes(1))
    }


    dStream.foreachRDD(rdd => {
      // 获取这个RDD的offset范围
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      var bl = true
      try {
        // 1：直接处理rdd -- 进行转化
        val keyValueRdd = rdd.map(processJsonData(_))
        // 2：处理一个分区的数据
        // 按数据库分组聚合
        keyValueRdd.groupByKey().foreachPartition(partition => {
          if (!partition.isEmpty) {
            partition.foreach(iter => {
              var key = iter._1
              var values = iter._2
              processEveryLine(key, values, spark, hiveDbName, groupId, dbConfigfile, hiveFilterTables)
            })
          }
        })
      } catch {
        case ex: Exception =>
          log.error(ex.getMessage, ex)
          ex.printStackTrace()
          bl = false
      } finally {
        if (bl) {
          for (o <- offsetRanges) {
            println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},untilOffset=${o.untilOffset}")
          }
          //手动提交offset,默认提交到Checkpoint中
          //recordDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
          var jdbc = connectInfoBC.value
          KafkaOffsetUtil.saveOffsetRanges(jdbc._1, jdbc._2, jdbc._3, groupIdBC.value, offsetRanges)
          //该RDD 异步提交
          dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
      }
    })

    //    val kafkaDStream = dStream.map(value => {
    //      var data =
    //        try {
    //          ReaderJsonData.readRecord(value.value)
    //        } catch {
    //          case _: Exception => Seq.empty
    //        }
    //      (data, value.value)
    //    })
    //    if (StringUtils.isNotBlank(errorDataPath)) {
    //      log.info(s"输出错误数据到$errorDataPath")
    //      kafkaDStream.filter(_._1.isEmpty).map(_._2).saveAsTextFiles(errorDataPath + "/error", "txt")
    //    }

    streamingContext.start

    val checkIntervalMillis = Minutes(1).milliseconds

    while (fileSystem.exists(processFilePath)) {
      streamingContext.awaitTerminationOrTimeout(checkIntervalMillis)
      if (!fileSystem.exists(processFilePath)) {
        log.info(s"进程文件【${processFilePath.toUri.getPath}】被删除,停止.")
        streamingContext.stop(stopSparkContext = true, stopGracefully = true)
      }
    }
    if (!fileSystem.exists(processFilePath) && streamingContext.getState() != StreamingContextState.STOPPED) {
      log.info(s"进程文件【${processFilePath.toUri.getPath}】被删除,停止.")
      streamingContext.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  private def getPropFileFromSparkConf(sparkConf: SparkConf, fileName: String): File = {
    val master = sparkConf.get("spark.master")
    val propFile = if (master.startsWith("local")) {
      getPropFile(sparkConf.get("spark.files"), fileName, _.getRawPath)
    } else if (master.equals("yarn")) {
      sparkConf.get("spark.submit.deployMode") match {
        case "client" => getPropFileAtYarn(sparkConf.get("spark.yarn.dist.files"), fileName, _.getRawPath)
        case "cluster" => getPropFileAtYarn(sparkConf.get("spark.yarn.dist.files"), fileName, uri => StringUtils.substringAfterLast(uri.getRawPath, File.separator))
        case _ => throw new RuntimeException("错误的spark.submit.deployMode.")
      }
    } else {
      throw new RuntimeException("错误的spark.master.")
    }
    log.info(s"读取配置文件:【$propFile】")
    propFile
  }

  private def getPropFile(filesStr: String, fileName: String, func: URI => String): File = {
    val propFileOpt = StringUtils.split(filesStr, ',').toList.find {
      uri =>
        val file = new File(uri)
        println(file.canRead + "--------->" + file.getName)
        file.canRead && file.getName.equalsIgnoreCase(fileName)
    }
    if (propFileOpt.isEmpty) {
      throw new RuntimeException
    } else {
      new File(propFileOpt.get)
    }
  }


  private def getPropFileAtYarn(filesStr: String, fileName: String, func: URI => String): File = {
    val propFileOpt = StringUtils.split(filesStr, ',').map(new URI(_)).find {
      uri =>
        val file = new File(func(uri))
        file.canRead && file.getName.equalsIgnoreCase(fileName)
    }
    if (propFileOpt.isEmpty) {
      throw new RuntimeException
    } else {
      new File(func(propFileOpt.get))
    }
  }

  private def getProperties(propertiesFile: File): Properties = {
    val propertiesTmp = new Properties
    propertiesTmp.load(Source.fromFile(propertiesFile).reader)
    propertiesTmp
  }
}
