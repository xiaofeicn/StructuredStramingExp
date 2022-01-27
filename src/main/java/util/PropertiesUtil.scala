package util

import java.io.FileInputStream
import java.util.Properties

/**
  * propertiesUtil
  *
  */
class PropertiesUtil extends Serializable {


  private val props = new Properties()

  var evn = ""
  // hdfs
  var hdfs_log_path = ""
  var hdfs_log_date = "0".toInt
  // hive
  var hiveDB = ""

  var log_filter = "true".toBoolean
  var streaming_filter = ""
  var streaming_close_port = ""
  var checkpoint_directory = ""


  // reids
  var redisMaxTotal = "0".toInt
  var redisMaxIdle = "0".toInt
  var redisMinIdle = "0".toInt
  var redisHost = ""
  var redisPort = "0".toInt
  var redisTimeout = "0".toInt
  var redispassport = ""
  var redisdbIndex = "0".toInt
  // mysql
  var mysqlHost = ""
  var mysqlUser = ""
  var mysqlPwd = ""
  // Kafka configurations
  var kafkaTopic = ""
  var kafkaBrokers = ""
  var kafkaGroupId = ""
  var kafkaGroupId2 = ""

  var prefix = ""
  // interval 计算频率
  var time_interval = "0".toInt

  //es
  var esNodes = ""
  var esIndex = ""


  // redis 缓存key
  var userHashKey = ""
  var resHashKey = ""
  // 特征向量缓存key
  var resFeatureHashKey = ""
  var userFeatureHashKey = ""
  var oheMapHashKey = ""

  var recallMixHashKey = ""
  var recallHotHashKey = ""


  var lrModelHashKey = ""

  var logLevel = "WARN"

  var logType = "0"
  def init(filePath: String): Unit = {
    props.load(new FileInputStream(filePath))

    // hdfs
    hdfs_log_path = props.getProperty("hdfs.log.path")
    hdfs_log_date = stringToInt(props.getProperty("hdfs.log.date"))
    // hive
    hiveDB = props.getProperty("hive.database")

    log_filter = if ("true".equals(props.getProperty("log.filter"))) true else false
    streaming_filter = props.getProperty("log.streaming.filter")

    // streaming
    streaming_close_port = props.getProperty("streaming.close.port")
    checkpoint_directory = props.getProperty("streaming.checkpoint.directory")



    // reids
    redisMaxTotal = stringToInt(props.getProperty("streaming.redis.maxTotal"))
    redisMaxIdle = stringToInt(props.getProperty("streaming.redis.maxIdle"))
    redisMinIdle = stringToInt(props.getProperty("streaming.redis.minIdle"))
    redisHost = props.getProperty("streaming.redis.redisHost")
    redisPort = stringToInt(props.getProperty("streaming.redis.redisPort"))
    redisTimeout = stringToInt(props.getProperty("streaming.redis.redisTimeout"))
    redispassport = props.getProperty("streaming.redis.passport")
    redisdbIndex = stringToInt(props.getProperty("streaming.redis.dbIndex"))
    // mysql
    mysqlHost = props.getProperty("mysql.host")
    mysqlUser = props.getProperty("mysql.user")
    mysqlPwd = props.getProperty("mysql.password")
    // Kafka configurations
    kafkaTopic = props.getProperty("streaming.kafka.topics")
    kafkaBrokers = props.getProperty("streaming.kafka.brokers")
    kafkaGroupId = props.getProperty("streaming.kafka.group.id")
    kafkaGroupId2 = props.getProperty("streaming.kafka.group.id2")

    prefix = props.getProperty("streaming.redis.key.prefix")


    // interval 计算频率
    time_interval = stringToInt(props.getProperty("streaming.time.interval"))

    //es
    esNodes = props.getProperty("es.nodes")
    esIndex = props.getProperty("es.index")

    logLevel = props.getProperty("log.level")
    logType = props.getProperty("log.type")

    // redis 缓存key
    userHashKey = prefix + ":log:streaming:user"
    resHashKey = prefix + ":log:streaming:resource"
    // 特征向量缓存key
    resFeatureHashKey = prefix + ":recmd:feature:res"
    userFeatureHashKey = prefix + ":recmd:feature:user"
    oheMapHashKey = prefix + ":recmd:feature:ohe"

    recallMixHashKey = prefix + ":recmd:recall:mix"
    recallHotHashKey = prefix + ":recmd:recall:hot"


    lrModelHashKey = prefix + ":recmd:model:lr"
  }

  def stringToInt(prop: String): Int = {
    try {
      prop.toInt
    } catch {
      case ex: Exception => {
        0
      }
    }
  }
}

//惰性单例，真正计算时才初始化对象
object PropertiesManager {
  @volatile private var propertiesUtil: PropertiesUtil = _

  def getUtil: PropertiesUtil = {
    propertiesUtil
  }

  def initUtil(evn: String): Unit = {
    var filePath = "config.properties"
    if (evn.contains("dev")) {
      filePath = this.getClass.getResource("/").toString.replace("file:", "") + "config_" + evn + ".properties"
    } else if (!evn.equals("")) {
      filePath = "config_" + evn + ".properties"
    }
    if (propertiesUtil == null) {
      propertiesUtil = new PropertiesUtil
    }
    propertiesUtil.init(filePath)
    propertiesUtil.evn = evn
  }
}
