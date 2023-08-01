package com.zdjizhi.flink.voip.records

import com.zdjizhi.flink.voip.records.Record._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

import scala.language.implicitConversions

/**
 * 用于解析和访问数据记录的类。
 *
 * @param obj 包含数据记录的 ObjectNode 对象
 * @constructor 创建一个 Record 对象，用于解析和访问数据记录
 * @author chaoc
 * @since 1.0
 */
class Record(obj: ObjectNode) {
  protected implicit val o: ObjectNode = obj
  /**
   * 数据记录中的所属 vsys
   */
  val vSysID: Int = getInt(F_COMMON_VSYS_ID)

  /**
   * 数据记录中的字段类型
   */
  val schemaType: String = getString(F_COMMON_SCHEMA_TYPE)

  /**
   * 数据记录中的流类型
   */
  val streamDir: Int = getInt(F_COMMON_STREAM_DIR)

  /**
   * 数据记录中的服务端地址字段值
   */
  val serverIp: String = getString(F_COMMON_SERVER_IP)

  /**
   * 数据记录中的服务端端口
   */
  val serverPort: Int = getInt(F_COMMON_SERVER_PORT)

  /**
   * 数据记录中的客户端地址
   */
  val clientIp: String = getString(F_COMMON_CLIENT_IP)

  /**
   * 数据记录中的客户端端口
   */
  val clientPort: Int = getInt(F_COMMON_CLIENT_PORT)

}

/**
 * 用于解析和访问数据记录。
 *
 * @see Record
 */
object Record {

  /**
   * 所属 vsys 字段名
   */
  val F_COMMON_VSYS_ID = "common_vsys_id"

  /**
   * 字段类型 字段名
   */
  val F_COMMON_SCHEMA_TYPE = "common_schema_type"

  /**
   * 流类型 字段名
   */
  val F_COMMON_STREAM_DIR = "common_stream_dir"

  /**
   * 服务端地址 字段名
   */
  val F_COMMON_SERVER_IP = "common_server_ip"

  /**
   * 服务端端口 字段名
   */
  val F_COMMON_SERVER_PORT = "common_server_port"

  /**
   * 客户端地址 字段名
   */
  val F_COMMON_CLIENT_IP = "common_client_ip"

  /**
   * 客户端端口 字段名
   */
  val F_COMMON_CLIENT_PORT = "common_client_port"

  /**
   * 从 ObjectNode 对象中获取整数类型字段值。
   *
   * @param field   字段名
   * @param default 默认值（可选，默认为 0）
   * @param o       包含字段的 ObjectNode 对象
   * @return 字段对应的整数值
   */
  def getInt(field: String, default: Int = 0)(implicit o: ObjectNode): Int = {
    o.get(field).asInt(default)
  }

  /**
   * 从 ObjectNode 对象中获取字符串类型字段值。
   *
   * @param field   字段名
   * @param default 默认值（可选，默认为 null）
   * @param o       包含字段的 ObjectNode 对象
   * @return 字段对应的字符串值
   */
  def getString(field: String, default: String = null)(implicit o: ObjectNode): String = {
    o.get(field).asText(default)
  }

  object Implicits {
    implicit def asRecord(o: ObjectNode): Record = {
      new Record(o)
    }
  }
}
