## Deploy

- 准备 JDK ${java.version} 的环境
- 准备 Flink ${flink.version} 的环境
- [下载](./download.html) 对应版本 UDF 依赖 Jar
- [下载](./download.html) 对应版本 Job 配置 (一个 yml 文件)
- 执行命令 `flink run -Dflink.rest.bind-port=8081 -c com.geedgenetworks.flink.easy.core.Runner path/to/sip-rtp-correlation-<version>.jar job.yml`
- 您将在控制台看到启动日志，同时您可以在 `http://<you-host>:8081` 看到任务 UI。

> 注意：
> SIP 和 RTP 融合作业强烈建议开启 Checkpoint 机制，否则将会由于丢失数据或重复数据导致业务数据关联错误。 