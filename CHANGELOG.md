# Changelog

### Hotfix
 - [#5](https://git.mesalab.cn/galaxy/tsg_olap/sip-rtp-correlation/-/issues/5) 修复了由于 IPUtil 在判断 Ipv6 地址没有判空而引起的空指针异常

### Feature
 - 输出 SIP Record
 - [GAL-419](https://jira.geedge.net/browse/GAL-419) 增加配置项 `include.intranet.ip`, 是否对 SIP 协商主叫 IP 或被叫 IP 为内网地址的数据进行关联，不关联则输出到异常 Topic 中。