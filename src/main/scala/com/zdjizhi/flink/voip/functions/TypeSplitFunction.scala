package com.zdjizhi.flink.voip.functions


import com.zdjizhi.flink.voip.functions.TypeSplitFunction._
import com.zdjizhi.flink.voip.records.Record.Implicits._
import com.zdjizhi.flink.voip.records.SchemaTypes._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * A ProcessFunction that splits ObjectNode records based on their 'schemaType' field.
 * It outputs SIP records to the 'sipSchemaTypeOutputTag' and RTP records to the 'rtpSchemaTypeOutputTag'.
 *
 * @author chaoc
 * @since 1.0
 */
class TypeSplitFunction extends ProcessFunction[ObjectNode, ObjectNode] {

  override def processElement(obj: ObjectNode,
                              ctx: ProcessFunction[ObjectNode, ObjectNode]#Context,
                              out: Collector[ObjectNode]): Unit = {
    // Split record based on its 'schemaType' field.
    obj.schemaType match {
      case SIP => ctx.output(sipSchemaTypeOutputTag, obj)
      case RTP => ctx.output(rtpSchemaTypeOutputTag, obj)
      case _ =>
    }
  }
}

/**
 * Defining the OutputTags for SIP and RTP records.
 */
object TypeSplitFunction {

  /**
   * OutputTag for SIP records.
   */
  val sipSchemaTypeOutputTag: OutputTag[ObjectNode] =
    OutputTag[ObjectNode]("schema-type-sip")

  /**
   * OutputTag for RTP records.
   */
  val rtpSchemaTypeOutputTag: OutputTag[ObjectNode] =
    OutputTag[ObjectNode]("schema-type-rtp")
}