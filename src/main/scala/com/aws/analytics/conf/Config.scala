package com.aws.analytics.conf

case class Config(
                     brokerList: String = "",
                     checkpointDir: String ="",
                     checkpointInterval:String ="60",
                     sourceTopic:String="",
                     groupId:String="",
                     tableName:String="",
                     warehouse:String=""
                   )


object Config {

    def parseConfig(obj: Object,args: Array[String]): Config = {
      val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
      val parser = new scopt.OptionParser[Config](programName) {
        head(programName, "1.0")
        opt[String]('c', "checkpointDir").required().action((x, config) => config.copy(checkpointDir = x)).text("checkpoint dir")
        opt[String]('l', "checkpointInterval").optional().action((x, config) => config.copy(checkpointInterval = x)).text("checkpoint interval: default 60 seconds")

        programName match {
          case "Kafka2Iceberg" =>
            opt[String]('b', "brokerList").required().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
            opt[String]('s', "sourceTopic").required().action((x, config) => config.copy(sourceTopic = x)).text("kafka source topic")
            opt[String]('g', "groupId").required().action((x, config) => config.copy(groupId = x)).text("consumer group id")
            opt[String]('t', "tableName").required().action((x, config) => config.copy(tableName = x)).text("iceberg glue tableName")
            opt[String]('w', "warehouse").required().action((x, config) => config.copy(warehouse = x)).text("iceberg s3 warehouse path")

          case _ =>

        }


      }
      parser.parse(args, Config()) match {
        case Some(conf) => conf
        case None => {
          //        println("cannot parse args")
          System.exit(-1)
          null
        }
      }

    }


}
