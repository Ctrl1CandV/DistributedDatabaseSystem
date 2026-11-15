# 分布式一致性与可用性演示

这是一个用 Python 和 MySQL 实现的学生记录数据库原型。所有节点运行相同程序，每个节点用独立的 MySQL 数据库存储 Raft 日志、任期、投票和学生记录。程序自动选出领导者，写入在多数副本持久化日志后才确认。节点恢复后通过日志复制自动追赶。

项目可在本机启动多个 Python 进程，也可分布在同一局域网的多台电脑上。节点数量由配置决定，课堂演示推荐三个节点。五个节点可以用于测试失去两个节点的场景。成员在启动时确定，同一组数据须一直使用同一份成员配置。

## 本机启动

以下示例使用 Windows PowerShell、Python 3.10+ 和 MySQL 8.0+。先安装依赖。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

在项目目录执行 `mysql -h 127.0.0.1 -u root -p`，用 MySQL 管理账户登录后执行下面的 SQL。示例假设数据库和账号尚未创建，密码换成自己的值。如果已创建空库或账号，跳过对应的创建语句并核对权限。已初始化的数据库在重启时直接复用。

```sql
CREATE USER 'ddb_demo'@'127.0.0.1' IDENTIFIED BY 'replace-with-your-password';
CREATE DATABASE ddb_node_1 CHARACTER SET utf8mb4;
CREATE DATABASE ddb_node_2 CHARACTER SET utf8mb4;
CREATE DATABASE ddb_node_3 CHARACTER SET utf8mb4;
GRANT SELECT, INSERT, UPDATE, DELETE ON ddb_node_1.* TO 'ddb_demo'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE, DELETE ON ddb_node_2.* TO 'ddb_demo'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE, DELETE ON ddb_node_3.* TO 'ddb_demo'@'127.0.0.1';
USE ddb_node_1;
SOURCE sql/init.sql
USE ddb_node_2;
SOURCE sql/init.sql
USE ddb_node_3;
SOURCE sql/init.sql
```

设置与建用户时相同的密码，再启动三个节点。账号和端口可通过 `run.py --help` 查看。启动器会保持存活的节点继续运行，按 Ctrl+C 停止这组进程。

```powershell
$env:DDB_MYSQL_PASSWORD = 'replace-with-your-password'
.\.venv\Scripts\python.exe run.py --nodes 3
```

另开终端并进入项目目录，查询任意节点的状态。选举可能需要几秒，看到 `leader_url` 后将其填入 `$leader`。状态中还可查看任期 `term`、日志末尾 `last_log_index` 和已提交位置 `commit_index`。

```powershell
$py = '.\.venv\Scripts\python.exe'
& $py -m ddb --server http://127.0.0.1:5001 status
$leader = 'http://127.0.0.1:5001'  # 用 status 的 leader_url 替换
& $py -m ddb --server $leader upsert 1001 Alice 90
& $py -m ddb --server $leader read 1001
& $py -m ddb --server $leader delete 1001
& $py -m ddb --server $leader read 1001
```

删除后读取成功，`record` 为 `null`。写入和默认的强一致读发向领导者。如果访问到跟随者，响应会给出它已知的领导者地址。等待多数派超时的写入结果可能未确认，先查询记录再决定是否重试。

指定任意健康节点并加上 `--mode stale`，即可读取该节点已提交的本地数据。节点落后或被隔离时，这种读取也可能成功并返回旧值。

```powershell
& $py -m ddb --server http://127.0.0.1:5002 read 1001 --mode stale
```

## 局域网部署

假设三台机器的局域网 IP 是 `192.168.1.11`、`192.168.1.12`、`192.168.1.13`。每台机器安装 Python 依赖，按本机的 SQL 示例准备一个空库（例如 `ddb_node_1`），授权应用账号，并在该库执行 `SOURCE sql/init.sql`。不同机器的本机库名可以相同。MySQL 可以只监听本机，其他节点只连接 Python 服务的 HTTP 端口。

将 [节点配置模板](config/node.example.json) 复制为各机器的 `config/node.json`。三台机器都要填写相同的 `cluster_id` 和同一套 `members`，成员地址使用各机器互相可达的 IP 和端口。每台机器设置各自的 `node_id`（依次为 `n1`、`n2`、`n3`）、MySQL 凭据和绑定端口。确保 TCP 5001 能在组内机器之间访问。

```powershell
$env:DDB_MYSQL_PASSWORD = '本机数据库账号密码'
.\.venv\Scripts\python.exe -m ddb serve --config config/node.json
```

客户端通过 `--server` 指定节点 URL，命令与本机演示相同。HTTP 接口未配置认证和 TLS，请在组内可信网络使用。在 Linux 或 macOS 上，使用 `python3 -m venv .venv` 和 `source .venv/bin/activate` 准备环境，再用 `python -m pip install -r requirements.txt` 安装依赖。执行 `export DDB_MYSQL_PASSWORD=...` 后，用 `python -m ddb serve --config config/node.json` 启动。

本机多个 schema 可以验证 Python 节点退出与恢复，但共享 MySQL 进程。测试单台 MySQL 故障，使用各自拥有 MySQL 的三台电脑，或在本机为不同节点配置独立 MySQL 实例。本机和三机使用不同数据库准备各自的集群。

## 开发与验收

| 文件 | 职责 |
| --- | --- |
| `ddb/raft.py` | 预投票、选主、日志复制、多数派提交、强一致读屏障 |
| `ddb/storage.py`、`sql/init.sql` | 持久化日志和投票，事务应用学生记录 |
| `ddb/http.py` | 节点 RPC 和学生读写 API |
| `ddb/config.py`、`ddb/cli.py` | 加载单节点配置、启动和访问节点 |
| `run.py`、`config/` | 本机多开和多机配置模板 |
| `tests/` | 可重复的故障场景和存储异常测试 |

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

课程论文和 PPT 可使用 [系统设计与算法](docs/architecture.md) 中的流程与边界，以及 [系统测试交接](docs/testing.md) 中的场景、步骤和记录项。来源与署名见 [SOURCE.md](SOURCE.md)。旧版的双副本数据库需保留在原 schema，新版请使用空库初始化。
