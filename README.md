# 简介

此包为 https://github.com/childe/gohangout 项目的 kafka inputs 插件。

# 特点

支持原有 inputs kafka 大部分参数，并添加了 kafka topic patterns
### 支持的kafka对应参数 参考https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
### 不支持的参数有

consumer_settings 下的 

- TLS 全部

# 使用方法

将 `ckafka_input.go` 复制到 `gohangout` 主目录下面, 运行

```bash
go build -buildmode=plugin -o ckafka_input.so ckafka_input.go
```

将 `ckafka_input.so` 路径作为 inputs

## gohangout 配置示例

### 指定消费者个数

```yaml
inputs:
    - '/path/go/ckafka_input.so': 
          topic:
            topicName: 5
            "^[a-z].*topic": 1 # 只要是以"^"开头的topic，一律为正则匹配
          codec: json
          consumer_settings:
            bootstrap.servers: '127.0.0.1:8080'
            group.id: groupId
            client.id: csw134s
            auto.commit.interval.ms: '1000'
            auto.commit: 'true'
outputs:
    - Stdout: {}
```

### 指定消费分区

每个分区一个消费者

```yaml
inputs:
    - '/path/go/ckafka_input.so': 
          assign:
            topicName: [1,3]:
          codec: json
          consumer_settings:
            bootstrap.servers: '127.0.0.1:8080'
            group.id: groupId
            client.id: csw134s
            auto.commit.interval.ms: '1000'
            auto.commit: 'true'
            version: '0.9.0.0'
outputs:
    - Stdout: {}
```

