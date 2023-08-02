package com.zdjizhi.flink.voip.records

import com.zdjizhi.flink.voip.records.Record._
import com.zdjizhi.flink.voip.records.SIPRecord._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

import scala.language.implicitConversions

/**
 * SIP（Session Initiation Protocol）数据记录类，用于解析和访问SIP数据记录。
 *
 * @param obj 包含 SIP 数据记录的 ObjectNode 对象
 * @constructor 创建一个 SIPRecord 对象，用于解析和访问 SIP 数据记录
 * @see Record
 * @author chaoc
 * @since 1.0
 */
class SIPRecord(obj: ObjectNode) extends Record(obj) {

  /**
   * SIP 通话的会话 ID 字段值
   */
  val callID: String = getString(F_CALL_ID)

  /**
   * SIP 通话的协调的主叫语音传输 IP 字段值
   */
  val originatorSdpConnectIp: String = getString(F_ORIGINATOR_SDP_CONNECT_IP)

  /**
   * SIP 通话的协调的主叫语音传输端口字段值
   */
  val originatorSdpMediaPort: Int = getInt(F_ORIGINATOR_SDP_MEDIA_PORT)

  /**
   * SIP 通话的协调的被叫语音传输 IP 字段值
   */
  val responderSdpConnectIp: String = getString(F_RESPONDER_SDP_CONNECT_IP)

  /**
   * SIP 通话的协调的被叫语音传输端口字段值
   */
  val responderSdpMediaPort: Int = getInt(F_RESPONDER_SDP_MEDIA_PORT)
}


/**
 * 用于解析和访问 SIP 数据记录。
 *
 * @see SIPRecord
 */
object SIPRecord {

  /**
   * 会话ID 字段名
   */
  val F_CALL_ID = "sip_call_id"
  /**
   * 协调的主叫语音传输IP 字段名
   */
  val F_ORIGINATOR_SDP_CONNECT_IP = "sip_originator_sdp_connect_ip"
  /**
   * 协调的主叫语音传输端口 字段名
   */
  val F_ORIGINATOR_SDP_MEDIA_PORT = "sip_originator_sdp_media_port"
  /**
   * 协调的被叫语音传输IP  字段名
   */
  val F_RESPONDER_SDP_CONNECT_IP = "sip_responder_sdp_connect_ip"
  /**
   * 协调的被叫语音传输端口 字段名
   */
  val F_RESPONDER_SDP_MEDIA_PORT = "sip_responder_sdp_media_port"

  private[voip] object Implicits {
    implicit def asSIPRecord(o: ObjectNode): SIPRecord = {
      new SIPRecord(o)
    }
  }
}