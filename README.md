# SIP RTP Correlation

## 简介

SIP RTP Correlation 项目是一个使用 Apache Flink 实现的实时数据处理项目，旨在从 Kafka 中读取 SIP（Session Initiation Protocol）和 RTP（Real-time Transport Protocol）数据，将它们融合成完整的 VoIP（Voice over Internet Protocol）通话数据。

SIP RTP Correlation 项目可以用于实时监控和分析 VoIP 通话数据，提取关键指标，以及进行实时报警和诊断。

## 编译和打包

使用Maven工具对项目进行编译和打包：
```shell
mvn clean package
```

## 运行Flink任务

使用以下命令运行Flink任务：
```shell
flink run -c com.zdjizhi.flink.voip.CorrelateApp path/to/sip-rtp-correlation-<version>.jar
```

## 配置项说明

TODO 待补充

## 贡献

如果您发现任何问题或改进项目的想法，欢迎提交 Issue 或 Pull Request。