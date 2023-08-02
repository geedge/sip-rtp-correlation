package com.zdjizhi.flink.voip.functions

import org.scalatest.flatspec.AnyFlatSpec

class AddressTest extends AnyFlatSpec {

  "Address.apply" should "return the Address with ports sorted and IPs reordered if necessary" in {
    val address1 = ("192.168.0.1", 80)
    val address2 = ("10.1.1.1", 8080)

    val result1 = Address(address1, address2)
    assert(result1 == Address("192.168.0.1", 80, "10.1.1.1", 8080))

    val result2 = Address(address2, address1)
    assert(result2 == Address("192.168.0.1", 80, "10.1.1.1", 8080))

    val address3 = ("172.16.0.1", 80)
    val result3 = Address(address3, address1)
    assert(result3 == Address("172.16.0.1", 80, "192.168.0.1", 80))

    val address4 = ("172.31.0.1", 443)
    val result4 = Address(address1, address4)
    assert(result4 == Address("192.168.0.1", 80, "172.31.0.1", 443))
  }

}
