# 代码来源

本项目由大学阶段共同开发的 [Ctrl1CandV/distributed-database](https://github.com/Ctrl1CandV/distributed-database) 提取和整理而来，原项目作者与共同贡献者的署名和贡献归属保留。

提取依据：原仓库 main 分支，提交 `82252e978f1a2e795c98326725327691dd8640dd`。

初次提取保留了 ZooKeeper 临时节点注册、子节点监听、双副本分配、表元数据保存、在线副本筛选与读写路由。

初次整理修复了表名、删表和容量回收问题，统一了 Python 客户端与 HTTP 接口，增加了 `all / available` 两种写入路由准入策略。

课程版本改用 Master 接收 HTTP 心跳，接入独立 MySQL 实例保存学生记录。写入按两个 Region 的事务结果确认，并支持两副本对比与手动补同步。未新增或变更原代码的开源许可声明。
