package com.zdjizhi.flink.voip.functions

import com.zdjizhi.utils.IPUtil
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

/**
 * A case class representing an address with two IP and port pairs.
 *
 * @param ip1   The first IP address.
 * @param port1 The first port number.
 * @param ip2   The second IP address.
 * @param port2 The second port number.
 * @author chaoc
 * @since 1.0
 */
case class Address(ip1: String, port1: Int, ip2: String, port2: Int)


object Address {

  /**
   * Creates an Address instance based on two tuples containing (String, Int) representing address information.
   * The method sorts the addresses based on the port number, and if the ports are equal, it sorts them based on
   * the numeric value of the IP address.
   *
   * @param address1 The first address information as a tuple (IP address, port).
   * @param address2 The second address information as a tuple (IP address, port).
   * @return An Address instance with addresses sorted and reordered.
   */
  def apply(address1: (String, Int), address2: (String, Int)): Address = {
    val seq = (address1 :: address2 :: Nil).sortWith {
      case (a, b) =>
        if (a._2 == b._2) {
          IPUtil.getIpDesimal(a._1) < IPUtil.getIpDesimal(b._1)
        } else {
          a._2 < b._2
        }
    }
    // Create an Address instance with the first address having the smaller port number,
    // and if the ports are equal, the smaller IP address comes first.
    Address(seq.head._1, seq.head._2, seq.last._1, seq.last._2)
  }

}

/**
 * A case class representing an ObjectNode with an expiration time.
 *
 * @param obj        The ObjectNode containing data.
 * @param expireTime The expiration time for the object.
 */
case class ObjectNodeWithExpiration(obj: ObjectNode, expireTime: Long)