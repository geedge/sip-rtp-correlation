package com.zdjizhi.flink.voip.functions

import com.zdjizhi.flink.voip.conf.FusionConfigs.RTP_STATE_CLEAR_INTERVAL
import com.zdjizhi.flink.voip.records.{Record, SchemaTypes, StreamDirs}
import org.apache.flink.api.common.time.Time
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

/**
 * The `VoIPFusionFunction` is a key-based co-process function that performs VoIP fusion logic
 * for SIP and RTP records. It combines SIP and RTP records belonging to the same session
 * and emits fused VoIP records. The function utilizes keyed state to store and manage SIP and
 * RTP records, and it uses timers to trigger regular clearing of the state.
 *
 * @author chaoc
 * @since 1.0
 */
class VoIPFusionFunction extends KeyedCoProcessFunction[Int, ObjectNode, ObjectNode, ObjectNode] with FunctionHelper {

  // The maximum number of RTP lines allowed per SIP for fusion.
  private val MAX_RTP_LINES: Int = 2
  private lazy val fireInterval: Time = Time.milliseconds(getGlobalConfiguration.get(RTP_STATE_CLEAR_INTERVAL))
  private lazy val sipDoubleState = getMapState("sip-state", classOf[Address], classOf[ObjectNodeWithInfo])
  private lazy val rtpState = getMapState("rtp-state", classOf[Address], classOf[ObjectNodeWithExpiration])

  override def processElement1(obj: ObjectNode,
                               ctx: KeyedCoProcessFunction[Int, ObjectNode, ObjectNode, ObjectNode]#Context,
                               out: Collector[ObjectNode]): Unit = {
    import com.zdjizhi.flink.voip.records.SIPRecord.Implicits._
    val address = Address((obj.originatorSdpConnectIp, obj.originatorSdpMediaPort),
      (obj.responderSdpConnectIp, obj.responderSdpMediaPort))

    sipDoubleState.put(address, ObjectNodeWithInfo(obj,
      ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds, 0))

    registerNextFireTimestamp(ctx.timerService(), fireInterval)
  }

  override def processElement2(obj: ObjectNode,
                               ctx: KeyedCoProcessFunction[Int, ObjectNode, ObjectNode, ObjectNode]#Context,
                               out: Collector[ObjectNode]): Unit = {
    import com.zdjizhi.flink.voip.records.Record.Implicits._
    val address = Address((obj.serverIp, obj.serverPort), (obj.clientIp, obj.clientPort))
    if (sipDoubleState.contains(address)) {
      val info = sipDoubleState.get(address)
      obj.merge(info.obj)
      obj.set(Record.F_COMMON_SCHEMA_TYPE, TextNode.valueOf(SchemaTypes.VoIP))
      out.collect(obj)

      obj.streamDir match {
        case StreamDirs.DOUBLE =>
          // In the context of VoIP fusion, only one RTP double directional stream
          sipDoubleState.remove(address)
        case _ =>
          // Save the number of fused RTP unidirectional streams
          sipDoubleState.put(address, info.copy(times = info.times + 1))
      }
    } else {
      rtpState.put(address, ObjectNodeWithExpiration(obj,
        ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds))
    }
    registerNextFireTimestamp(ctx.timerService(), fireInterval)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedCoProcessFunction[Int, ObjectNode, ObjectNode, ObjectNode]#OnTimerContext,
                       out: Collector[ObjectNode]): Unit = {
    import com.zdjizhi.flink.voip.records.Record.Implicits._
    val iterator = rtpState.iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val obj = entry.getValue.obj
      val address = entry.getKey

      if (sipDoubleState.contains(address)) {
        val info = sipDoubleState.get(address)
        obj.streamDir match {
          case StreamDirs.DOUBLE =>
            sipDoubleState.remove(address)
          case _ if info.times >= MAX_RTP_LINES - 1 =>
            // One RTP unidirectional stream has already been fused
            sipDoubleState.remove(address)
        }
        obj.merge(info.obj)
          .set(Record.F_COMMON_SCHEMA_TYPE, TextNode.valueOf(SchemaTypes.VoIP))
        out.collect(obj)
      }

      if (entry.getValue.expireTime <= timestamp) {
        rtpState.remove(entry.getKey)
      }
    }
    sipDoubleState.iterator().forEachRemaining(entry => {
      if (entry.getValue.expireTime <= timestamp) {
        sipDoubleState.remove(entry.getKey)
      }
    })
    registerNextFireTimestamp(ctx.timerService(), fireInterval)
  }
}