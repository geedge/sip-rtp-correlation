package com.zdjizhi.flink.voip

import org.apache.flink.api.java.utils.ParameterTool

import scala.language.implicitConversions

package object conf {

  implicit def asFusionParameterTool(tool: ParameterTool): FusionParameterTool = new FusionParameterTool(tool)

}
