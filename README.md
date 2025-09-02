# hissrv - 历史数据服务

历史数据存储和管理，将实时数据持久化到时序数据库并提供查询服务。

## 功能特性

- **数据收集**: 定时从Redis获取实时数据
- **数据存储**: 将数据持久化到InfluxDB 2.7时序数据库
- **分页查询**: 提供高效的历史数据查询API
- **统计分析**: 支持多种聚合函数和时间间隔的统计查询
- **健康监控**: 完整的系统健康检查和监控
- **定时任务**: 自动化数据收集、清理和统计任务

## 系统架构

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│    Redis    │───▶│   hissrv    │───▶│  InfluxDB   │
│  实时数据   │    │ 历史数据服务 │    │  时序数据库  │
└─────────────┘    └─────────────┘    └─────────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │   API接口   │
                   │ /hisApi/... │
                   └─────────────┘
```

## 数据流程

1. **数据收集**: 定时从Redis扫描匹配的数据模式
   - `comsrv:*:T` - 通信服务遥测数据
   - `comsrv:*:S` - 通信服务遥信数据  
   - `comsrv:*:C` - 通信服务遥控数据
   - `comsrv:*:A` - 通信服务遥调数据
   - `modsrv:model:*:measurement` - 物模型测量数据

2. **数据转换**: 将Redis Hash数据转换为InfluxDB时序数据点

3. **批量存储**: 批量写入InfluxDB以提高性能

4. **查询服务**: 提供RESTful API查询历史数据

## 快速开始

### 1. 环境要求

- Python 3.10+
- Redis (数据源)
- InfluxDB 2.7+ (数据存储)

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

复制 `.envshow` 文件为 `.env` 并修改配置：

```bash
cp .envshow .env
# 编辑 .env 文件，配置Redis和InfluxDB连接信息
```

### 4. 配置服务

编辑 `config/hissrv.yaml` 文件，配置：
- InfluxDB连接信息
- 数据收集策略
- 定时任务设置

### 5. 启动服务

```bash
python main.py
```

服务将在 `http://localhost:6004` 启动。

## API接口

### 基础信息
- 服务端口: `6004`
- API前缀: `/hisApi`
- API文档: `http://localhost:6004/docs`

### 主要接口

#### 1. 健康检查
```
GET /hisApi/health
```

#### 2. 查询历史数据
```
GET /hisApi/data/query?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z&page=1&page_size=100
```

参数：
- `start_time`: 开始时间 (ISO 8601格式)
- `end_time`: 结束时间 (ISO 8601格式) 
- `channel_ids`: 通道ID列表 (可选, 逗号分隔)
- `point_ids`: 点位ID列表 (可选, 逗号分隔)
- `data_types`: 数据类型列表 (可选, T/S/C/A/M)
- `sources`: 数据来源列表 (可选, comsrv/modsrv)
- `page`: 页码 (默认: 1)
- `page_size`: 每页大小 (默认: 100, 最大: 1000)

#### 3. 统计查询
```
GET /hisApi/data/statistics?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z&channel_id=1001&point_id=1&data_type=T&aggregation=mean&interval=1h
```

#### 4. 获取最新数据
```
GET /hisApi/data/latest?channel_id=1001&point_id=1&data_type=T
```

#### 5. 系统指标
```
GET /hisApi/metrics
```

## 配置说明

### InfluxDB配置

```yaml
influxdb:
  url: "http://localhost:8086"
  token: "your-influxdb-token"
  org: "your-org" 
  bucket: "history_data"
  timeout: 30
  batch_size: 1000
  flush_interval: 10
```

### 定时任务配置

```yaml
scheduler:
  data_collection:
    enabled: true
    interval: 5  # 秒
    batch_size: 1000
  data_cleanup:
    enabled: true
    cron: "0 2 * * *"
    cleanup_older_than: "30d"
  statistics:
    enabled: true
    cron: "0 1 * * *"
  health_check:
    enabled: true
    interval: 60
```

### Redis数据源配置

```yaml
redis_source:
  subscribe_patterns:
    - "comsrv:*:T"
    - "comsrv:*:S"
    - "comsrv:*:C"
    - "comsrv:*:A"
    - "modsrv:model:*:measurement"
    - "modsrv:model:*:action"
  filters:
    enabled: true
    exclude_patterns:
      - "*:debug:*"
      - "*:temp:*"
```

## 数据模型

### Redis数据格式
```
# 遥测数据示例
comsrv:1001:T
  1 -> "25.5"      # 温度值
  2 -> "380.2"     # 电压值
  3 -> "12.8"      # 电流值
  _timestamp -> "1704067200"

# 物模型数据示例  
modsrv:model:transformer1:measurement
  temperature -> "25.5"
  voltage_a -> "380.2"
  current_a -> "12.8"
  __updated -> "1704067200"
```

### InfluxDB存储格式
```
measurement: telemetry
tags:
  - channel_id: "1001"
  - point_id: "1"
  - data_type: "T"
  - source: "comsrv"
  - quality: "GOOD"
fields:
  - value: 25.5
time: 2024-01-01T00:00:00Z
```

## 监控和运维

### 1. 健康检查
```bash
curl http://localhost:6004/hisApi/health
```

### 2. 查看调度器状态
```bash
curl http://localhost:6004/hisApi/scheduler/status
```

### 3. 手动执行任务
```bash
# 手动执行数据收集
curl -X POST http://localhost:6004/hisApi/scheduler/run/collect_and_store

# 手动执行健康检查
curl -X POST http://localhost:6004/hisApi/scheduler/run/health_check
```

### 4. 查看系统指标
```bash
curl http://localhost:6004/hisApi/metrics
```

## 性能优化

1. **批量写入**: 使用批量写入提高InfluxDB写入性能
2. **时间分区**: InfluxDB自动按时间分区存储数据
3. **索引优化**: 合理设置tag字段用于快速查询
4. **内存管理**: 控制批处理大小避免内存溢出
5. **连接池**: 复用数据库连接减少开销

## 故障排除

### 1. Redis连接失败
- 检查Redis服务状态
- 验证连接配置（主机、端口、密码）
- 检查网络连通性

### 2. InfluxDB连接失败  
- 检查InfluxDB服务状态
- 验证Token和权限
- 检查Bucket是否存在

### 3. 数据收集异常
- 检查Redis数据格式
- 查看日志文件排查错误
- 验证订阅模式配置

### 4. 查询性能慢
- 缩小查询时间范围
- 使用更具体的过滤条件
- 检查InfluxDB索引状态

## 开发说明

### 项目结构
```
hissrv/
├── app/                    # 应用源码
│   ├── api/               # API路由
│   ├── core/              # 核心模块
│   ├── models/            # 数据模型
│   ├── services/          # 业务服务
│   └── main.py            # 主应用
├── config/                # 配置文件
├── logs/                  # 日志目录
├── requirements.txt       # 依赖列表
├── main.py               # 启动脚本
└── README.md             # 说明文档
```

### 扩展开发
1. **添加新数据源**: 在 `DataCollector` 中扩展数据收集逻辑
2. **自定义查询**: 在 `QueryService` 中添加新的查询方法
3. **新增API**: 在 `routes.py` 中定义新的接口
4. **定时任务**: 在 `SchedulerService` 中添加新任务

## License

MIT License