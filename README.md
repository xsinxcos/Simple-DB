# Simple-DB (myDB)

一个用Java编写的简单数据库实现。

## 概述

Simple-DB是一个从零开始构建的轻量级数据库系统。它提供了基本的数据库功能，采用客户端-服务器架构。

## 项目结构

- `backend`: 数据库核心引擎实现
- `client`: 连接数据库的客户端接口
- `common`: 共享工具和通用功能
- `transport`: 客户端-服务器通信的网络传输层

## 环境要求

- Java 8或更高版本
- Maven用于依赖管理

## 依赖项

- JUnit (用于测试)
- Google Guava
- Google Gson
- Apache Commons Codec
- Apache Commons CLI

## 构建项目

```bash
mvn clean package
```
