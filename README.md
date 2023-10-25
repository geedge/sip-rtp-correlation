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
flink run -c com.zdjizhi.flink.voip.CorrelateApp path/to/sip-rtp-correlation-<version>.jar application.properties
```

## 配置项说明

| 配置项                      | 类型                | 必需 | 默认值 | 描述                                         |
| --------------------------- | ------------------- | ---------- | ----------------------------------------------------------- |--------------------------------------------|
| source.kafka.topic          | STRING              | Y |  | 将要读取的 Kafka Topic 名称，其包含 SIP 和 RTP 原始数据    |
| source.kafka.props.*        | MAP<STRING, STRING> | Y |  | 将要读取的 Kafka 的 Properties                   |
| sink.kafka.topic            | STRING              | Y |  | 将合成的 VoIP 及 未关联成功的 RTP 数据写出的 Kafka Topic 名 |
| sink.kafka.props.*          | MAP<STRING, STRING> | Y |  | 数据输出的 Kafka 的 Properties                   |
| error.records.output.enable | BOOLEAN   | N | False | 是否开启异常数据的输出 【IP 或 Port 为空】                 |
| include.intranet.ip | BOOLEAN | N | True | 是否对 SIP 协商主叫 IP 或被叫 IP 为内网地址的数据进行关联        |
| error.sink.kafka.topic      | STRING              | N |      | 异常数据输出到的 Kafka Topic 名                     |
| error.sink.kafka.props.*    | MAP<STRING, STRING> | N |      | 异常数据输出的 Kafka 的 Properties                 |
| sip.state.clear.interval.minutes | INT | N | 1 | SIP 单向流关联的窗口大小（单位：分钟）                      |
| rtp.state.clear.interval.minutes | INT | N | 6 | SIP 和 RTP 关联的窗口大小（单位：分钟）                   |



## 贡献

如果您发现任何问题或改进项目的想法，欢迎提交 Issue 或 Pull Request。