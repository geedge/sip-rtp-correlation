package com.zdjizhi.flink.voip.data

import com.github.javafaker.Faker
import com.zdjizhi.flink.voip.data.Generator.random

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

// 生成数据，有效数据即可以双向匹配的数据占比 [ratio/100]
abstract class Generator[T](ratio: Int) {

  require(ratio >= 0 && ratio <= 100,
    "Param 'ratio' is limited to 0-100 integers only.")

  private lazy val state = new AtomicReference[T]()

  final def next: T = {
    val i = random.nextInt(100)
    val v = if (i < ratio && state.get() != null) {
      afterState(state.get())
    } else {
      state.updateAndGet(_ => this.generate)
    }
    state.set(null.asInstanceOf[T])
    afterAll(v)
  }

  protected def generate: T

  protected def afterState(v: T): T

  protected def afterAll(t: T): T = t

}

object Generator {

  val random: ThreadLocalRandom = ThreadLocalRandom.current()

  private val faker = new Faker()

  def nextIP: String = {
    if (random.nextBoolean()) {
      faker.internet().ipV4Address()
    } else {
      faker.internet().ipV6Address()
    }
  }

  def nextID: String = faker.idNumber().valid()

  def nextPort: Int = random.nextInt(65535)

}