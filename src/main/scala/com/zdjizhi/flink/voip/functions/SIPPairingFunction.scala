package com.zdjizhi.flink.voip.functions

import com.zdjizhi.flink.voip.conf.FusionConfigs.SIP_STATE_CLEAR_INTERVAL
import com.zdjizhi.flink.voip.records.SIPRecord.Implicits._
import com.zdjizhi.flink.voip.records.{Record, StreamDirs}
import org.apache.flink.api.common.time.Time
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{IntNode, ObjectNode}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * A KeyedProcessFunction that pairs SIP records based on their addresses and stream direction.
 * SIP records are paired when they have the same addresses but opposite stream directions.
 *
 * @author chaoc
 * @since 1.0
 */
class SIPPairingFunction extends KeyedProcessFunction[(Int, String), ObjectNode, ObjectNode]
  with FunctionHelper {

  private lazy val fireInterval: Time = Time.milliseconds(getGlobalConfiguration.get(SIP_STATE_CLEAR_INTERVAL))
  // A MapState to store SIP records with their addresses and expiration time.
  private lazy val mapState = getMapState("sip-state", classOf[Address], classOf[ObjectNodeWithExpiration])

  override def processElement(obj: ObjectNode,
                              ctx: KeyedProcessFunction[(Int, String), ObjectNode, ObjectNode]#Context,
                              out: Collector[ObjectNode]): Unit = {
    // Register a timer for the next clearing interval.
    registerNextFireTimestamp(ctx.timerService(), fireInterval)

    // When SIP is a one-way stream.
    if (obj.streamDir != StreamDirs.DOUBLE) {
      // Create an Address instance based on server and client IPs and ports.
      val address = Address((obj.serverIp, obj.serverPort), (obj.clientIp, obj.clientPort))
      // If the address is already stored in the mapState and has the opposite stream direction,
      // merge the SIP records, change the stream direction to DOUBLE, and output the merged record.
      if (mapState.contains(address) && mapState.get(address).obj.streamDir != obj.streamDir /* TODO consider stream direction */ ) {
        obj.merge(mapState.get(address).obj)
        obj.set(Record.F_COMMON_STREAM_DIR, IntNode.valueOf(StreamDirs.DOUBLE))
        out.collect(obj)
        mapState.remove(address)
      } else {
        // If the address is not yet in the mapState, add it with its expiration time.
        val value = ObjectNodeWithExpiration(obj,
          ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds)
        mapState.put(address, value)
      }
    } else {
      // If SIP is a double stream, pairing isn't required, directly output the record.
      out.collect(obj)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[(Int, String), ObjectNode, ObjectNode]#OnTimerContext,
                       out: Collector[ObjectNode]): Unit = {

    val iterator = mapState.entries().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      // Remove expired entries from the mapState based on their expiration time.
      if (entry.getValue.expireTime <= timestamp) {
        mapState.remove(entry.getKey)
      }
    }
    registerNextFireTimestamp(ctx.timerService(), fireInterval)
  }
}
