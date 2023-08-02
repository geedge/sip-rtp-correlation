package com.zdjizhi.flink.voip

import com.zdjizhi.flink.voip.conf._
import com.zdjizhi.flink.voip.functions.TypeSplitFunction._
import com.zdjizhi.flink.voip.functions.{SIPPairingFunction, TypeSplitFunction, VoIPFusionFunction}
import com.zdjizhi.flink.voip.records.SIPRecord.Implicits._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * App Main.
 *
 * @author chaoc
 * @since 1.0
 */
object FusionApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    require(args.nonEmpty, "Error: Not found properties path. \nUsage: flink -c xxx xxx.jar app.properties.")

    val tool = ParameterTool.fromPropertiesFile(args.head)

    env.getConfig.setGlobalJobParameters(tool.getConfiguration)

    val consumer = new FlinkKafkaConsumer[ObjectNode](
      tool.getConfiguration.get(FusionConfigs.SOURCE_KAFKA_TOPIC),
      new JsonNodeDeserializationSchema,
      tool.getProperties(FusionConfigs.SOURCE_KAFKA_PROPERTIES_PREFIX))

    val schemaTypeSplitStream = env.addSource(consumer)
      .process(new TypeSplitFunction)

    val sipStream = schemaTypeSplitStream.getSideOutput(sipSchemaTypeOutputTag)

    val sipDoubleStream = sipStream.keyBy { i => (i.vSysID, i.callID) }
      .process(new SIPPairingFunction)

    val rtpStream = schemaTypeSplitStream.getSideOutput(rtpSchemaTypeOutputTag)

    rtpStream.keyBy(_.vSysID).connect(sipDoubleStream.keyBy(_.vSysID))
      .process(new VoIPFusionFunction)

    env.execute("VoIP Fusion Job")
  }

}
