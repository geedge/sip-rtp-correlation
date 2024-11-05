## Download

### ${project.version}

- [TSG-23174](https://jira.geedge.net/browse/TSG-23174) 测试完成，升级为稳定版本。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| ${easy.stream.version}         | [JAR](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/${project.version}/${project.artifactId}-${project.version}.jar) ( [MD5](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/${project.version}/${project.artifactId}-${project.version}.jar.md5) [SHA1](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/${project.version}/${project.artifactId}-${project.version}.jar.sha1) ) | [YML](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/${project.version}/${project.artifactId}-${project.version}.tar.gz) ( [MD5](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/${project.version}/${project.artifactId}-${project.version}.tar.gz.md5) [SHA1](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/${project.version}/${project.artifactId}-${project.version}.tar.gz.sha1) ) |


- 修复 sip 双向关联模块的数据丢失问题。
- [GAL-684](https://jira.geedge.net/browse/GAL-684) 调整融合后的字段映射，增加更加详细的监控指标。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| ${easy.stream.version}         | [JAR](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc9/${project.artifactId}-2.0-rc9.jar) ( [MD5](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc9/${project.artifactId}-2.0-rc9.jar.md5) [SHA1](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc9/${project.artifactId}-2.0-rc9.jar.sha1) ) | [YML](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc9/${project.artifactId}-2.0-rc9.tar.gz) ( [MD5](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc9/${project.artifactId}-2.0-rc9.tar.gz.md5) [SHA1](${project.distributionManagement.repository.url}/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc9/${project.artifactId}-2.0-rc9.tar.gz.sha1) ) |

### 2.0-rc8

- 修复 Extract Key 错误，由于 IpAddress 类型数据校验导致的空指针异常。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.3-rc1         | [JAR](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc8/sip-rtp-correlation-2.0-rc8.jar) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc8/sip-rtp-correlation-2.0-rc8.jar.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc8/sip-rtp-correlation-2.0-rc8.jar.sha1) ) | [YML](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc8/sip-rtp-correlation-2.0-rc8.tar.gz) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc8/sip-rtp-correlation-2.0-rc8.tar.gz.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc8/sip-rtp-correlation-2.0-rc8.tar.gz.sha1) ) |


### 2.0-rc7

- VoIP Record 增加字段: SIP `sip_bye_reason` 相关字段。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.3-rc1         | [JAR](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc7/sip-rtp-correlation-2.0-rc7.jar) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc7/sip-rtp-correlation-2.0-rc7.jar.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc7/sip-rtp-correlation-2.0-rc7.jar.sha1) ) | [YML](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc7/sip-rtp-correlation-2.0-rc7.tar.gz) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc7/sip-rtp-correlation-2.0-rc7.tar.gz.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc7/sip-rtp-correlation-2.0-rc7.tar.gz.sha1) ) |


### 2.0-rc6

- VoIP Record 增加字段: SIP， RTP `protocol` 相关字段。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.3-rc1         | [JAR](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc6/sip-rtp-correlation-2.0-rc6.jar) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc6/sip-rtp-correlation-2.0-rc6.jar.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc6/sip-rtp-correlation-2.0-rc6.jar.sha1) ) | [YML](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc6/sip-rtp-correlation-2.0-rc6.tar.gz) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc6/sip-rtp-correlation-2.0-rc6.tar.gz.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc6/sip-rtp-correlation-2.0-rc6.tar.gz.sha1) ) |

### 2.0-rc5

- 修复 VoIP 字段错误，使用 `decoded_as` 表示 VoIP 日志。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.3-rc1     | [JAR](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc5/sip-rtp-correlation-2.0-rc5.jar) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc5/sip-rtp-correlation-2.0-rc5.jar.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc5/sip-rtp-correlation-2.0-rc5.jar.sha1) ) | [YML](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc5/sip-rtp-correlation-2.0-rc5.tar.gz) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc5/sip-rtp-correlation-2.0-rc5.tar.gz.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc5/sip-rtp-correlation-2.0-rc5.tar.gz.sha1) ) |

### 2.0-rc4

- 移除对 `log_id`, `recv_time` 字段的强制非空校验, 该字段在后续步骤中处理。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.3-rc1     | [JAR](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc4/sip-rtp-correlation-2.0-rc4.jar) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc4/sip-rtp-correlation-2.0-rc4.jar.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc4/sip-rtp-correlation-2.0-rc4.jar.sha1) ) | [YML](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc4/sip-rtp-correlation-2.0-rc4.tar.gz) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc4/sip-rtp-correlation-2.0-rc4.tar.gz.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc4/sip-rtp-correlation-2.0-rc4.tar.gz.sha1) ) |

### 2.0-rc3

- 修复由于 State 过期策略早于 Timer 触发而导致的未关联成功的 RTP 数据未正常输出的问题。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.3-rc1     | [JAR](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc3/sip-rtp-correlation-2.0-rc3.jar) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc3/sip-rtp-correlation-2.0-rc3.jar.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc3/sip-rtp-correlation-2.0-rc3.jar.sha1) ) | [YML](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc3/sip-rtp-correlation-2.0-rc3.tar.gz) ( [MD5](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc3/sip-rtp-correlation-2.0-rc3.tar.gz.md5) [SHA1](http://192.168.40.153:8081/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc3/sip-rtp-correlation-2.0-rc3.tar.gz.sha1) ) |

### 2.0-rc2

- 修复由于 Flink 1.13.6 和 Flink 1.13.1 之间某些 Runtime 不兼容的问题导致的系统无法启动。

| Easy Stream | UDF Jar                                                      | Job                                                          |
|-------------| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.3-rc1     | [JAR](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc2/sip-rtp-correlation-2.0-rc2.jar) ( [MD5](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc2/sip-rtp-correlation-2.0-rc2.jar.md5) [SHA1](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc2/sip-rtp-correlation-2.0-rc2.jar.sha1) ) | [YML](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc2/sip-rtp-correlation-2.0-rc2.tar.gz) ( [MD5](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc2/sip-rtp-correlation-2.0-rc2.tar.gz.md5) [SHA1](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc2/sip-rtp-correlation-2.0-rc2.tar.gz.sha1) ) |

### 2.0-rc1

| Easy Stream | UDF Jar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Job                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1.3-rc1     | [JAR](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc1/sip-rtp-correlation-2.0-rc1.jar) ( [MD5](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc1/sip-rtp-correlation-2.0-rc1.jar.md5) [SHA1](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc1/sip-rtp-correlation-2.0-rc1.jar.sha1) ) | [YML](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc1/sip-rtp-correlation-2.0-rc1.tar.gz) ( [MD5](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc1/sip-rtp-correlation-2.0-rc1.tar.gz.md5) [SHA1](http://192.168.40.153:8099/content/repositories/platform-release/com/geedgenetworks/application/sip-rtp-correlation/2.0-rc1/sip-rtp-correlation-2.0-rc1.tar.gz.sha1) ) |

