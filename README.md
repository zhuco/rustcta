# Rust CTA 多交易所虚拟货币 交易系统

一个基于 Rust 开发的加密货币量化交易系统，内置网格交易和 Avellaneda-Stoikov 做市策略。
目前支持币安交易所U本位永续合约

## 功能特性

- 🔄 **网格交易策略**: 自动化网格交易，支持动态调整
- 📊 **Avellaneda-Stoikov 做市策略**: 基于学术理论的做市算法
- 🔌 **多交易所支持**: 目前支持币安交易所
- 📡 **实时数据**: WebSocket 实时价格和订单更新
- 📝 **完整日志**: 毫秒级时间戳，文件和控制台双输出
- ⚙️ **灵活配置**: JSON 配置文件，支持多交易对
- 🐳 **容器化部署**: Docker 支持，跨平台编译

## 系统要求

- Rust 1.70+
- OpenSSL 开发库
- 网络连接（访问交易所 API）

## 快速开始

### 1. 克隆项目

```bash
git clone <repository-url>
cd rustcta
```

### 2. 配置设置

复制配置模板并修改：

```bash
cp config/config.json.example config/config.json
cp config/api_keys.json.example config/api_keys.json
```

编辑 `config/api_keys.json` 添加你的 API 密钥：

```json
{
  "binance": {
    "api_key": "your_api_key_here",
    "secret_key": "your_secret_key_here"
  }
}
```

### 3. 本地运行

```bash
# 开发模式
cargo run

# 发布模式
cargo run --release
```

### 4. 快速编译 Linux 版本（推荐）

如果你需要在 Linux 服务器上运行，可以使用提供的一键编译脚本：

```bash
# Windows 用户：双击运行或在命令行执行
build-linux.bat

# 或者手动执行 Docker 编译
docker build -t rustcta-builder .
docker create --name temp-container rustcta-builder
docker cp temp-container:/app/target/x86_64-unknown-linux-gnu/release/rustcta ./rustcta-linux
docker rm temp-container
```

编译完成后，`rustcta-linux` 文件可以直接在 Linux 服务器上运行。

## 多平台编译

### 方法一：Docker 交叉编译（推荐）

#### 使用项目 Dockerfile 编译 Linux 可执行文件

```bash
# 方法1：使用项目自带的 Dockerfile
docker build -t rustcta-builder .
docker create --name temp-container rustcta-builder
docker cp temp-container:/app/target/x86_64-unknown-linux-gnu/release/rustcta ./rustcta-linux
docker rm temp-container

设置权限
chmod +x rustcta-linux

运行程序
./rustcta-linux
```

#### 一键编译脚本（Windows）

```bash
# 创建编译脚本 build-linux.bat
@echo off
echo 正在使用 Docker 编译 Linux 版本...
docker build -t rustcta-builder .
if %errorlevel% neq 0 (
    echo Docker 构建失败！
    pause
    exit /b 1
)

echo 正在提取可执行文件...
docker create --name temp-container rustcta-builder
docker cp temp-container:/app/target/x86_64-unknown-linux-gnu/release/rustcta ./rustcta-linux
docker rm temp-container

echo 编译完成！Linux 可执行文件: rustcta-linux
pause
```

#### 手动 Docker 编译（如果需要自定义）

```bash
# 使用官方 Rust 镜像进行交叉编译
docker run --rm -v "%cd%":/workspace -w /workspace rust:1.88 bash -c "
  apt-get update && 
  apt-get install -y pkg-config libssl-dev gcc-x86-64-linux-gnu libc6-dev-amd64-cross && 
  rustup target add x86_64-unknown-linux-gnu && 
  export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc && 
  export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc && 
  export CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ && 
  cargo build --release --target x86_64-unknown-linux-gnu
"

# 提取可执行文件
copy target\x86_64-unknown-linux-gnu\release\rustcta rustcta-linux
```

#### macOS 可执行文件编译

```bash
# 添加 macOS 目标
rustup target add x86_64-apple-darwin

# 使用 osxcross 工具链编译（需要额外设置）
# 或使用 GitHub Actions 在 macOS 环境中编译
```

#### Windows 可执行文件编译

```bash
# 添加 Windows 目标
rustup target add x86_64-pc-windows-gnu

# 安装 MinGW 工具链
# Ubuntu/Debian:
sudo apt-get install gcc-mingw-w64-x86-64

# 编译
cargo build --release --target x86_64-pc-windows-gnu
```

### 方法二：本地交叉编译

#### 安装目标平台

```bash
# Linux
rustup target add x86_64-unknown-linux-gnu

# macOS
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin

# Windows
rustup target add x86_64-pc-windows-gnu
rustup target add x86_64-pc-windows-msvc
```

#### 编译命令

```bash
# Linux
cargo build --release --target x86_64-unknown-linux-gnu

# macOS Intel
cargo build --release --target x86_64-apple-darwin

# macOS Apple Silicon
cargo build --release --target aarch64-apple-darwin

# Windows GNU
cargo build --release --target x86_64-pc-windows-gnu

# Windows MSVC
cargo build --release --target x86_64-pc-windows-msvc
```

### 方法三：GitHub Actions 自动化编译

在 `.github/workflows/build.yml` 中配置多平台自动编译：

```yaml
name: Build Multi-Platform

on: [push, pull_request]

jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
    runs-on: ${{ matrix.os }}
    
    steps:
    - uses: actions/checkout@v3
    - uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
    - name: Build
      run: cargo build --release
    - name: Upload artifacts
      uses: actions/upload-artifact@v3
      with:
        name: rustcta-${{ matrix.os }}
        path: target/release/rustcta*
```

## 部署

### Linux 服务器部署

项目提供了完整的 Linux 部署包，包含：

- `rustcta-linux`: Linux 可执行文件
- `config/`: 配置文件目录
- `run.sh`: 启动脚本
- `rustcta.service`: systemd 服务文件
- `DEPLOY.md`: 详细部署说明

生成部署包：

```bash
# 创建部署目录
mkdir linux-deploy

# 复制文件
copy rustcta-linux linux-deploy/
xcopy config linux-deploy/config/ /E /I
copy run.sh linux-deploy/
copy rustcta.service linux-deploy/
copy DEPLOY.md linux-deploy/
```

详细部署步骤请参考 `linux-deploy/DEPLOY.md`。

### Docker 部署

#### 开发环境 Docker 使用

```bash
# 构建开发镜像
docker build -t rustcta-dev .

# 运行开发容器（Windows）
docker run -d --name rustcta-dev ^
  -v "%cd%\config:/app/config" ^
  -v "%cd%\log:/app/log" ^
  rustcta-dev

# 运行开发容器（Linux/macOS）
docker run -d --name rustcta-dev \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/log:/app/log \
  rustcta-dev
```

#### 生产环境 Docker 部署

```bash
# 1. 编译 Linux 可执行文件
docker build -t rustcta-builder .
docker create --name temp-container rustcta-builder
docker cp temp-container:/app/target/x86_64-unknown-linux-gnu/release/rustcta ./rustcta-linux
docker rm temp-container

# 2. 创建生产环境 Dockerfile
cat > Dockerfile.prod << EOF
FROM ubuntu:22.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# 创建应用目录
WORKDIR /app

# 复制可执行文件
COPY rustcta-linux /app/rustcta
RUN chmod +x /app/rustcta

# 创建配置和日志目录
RUN mkdir -p /app/config /app/log

# 暴露端口（如果需要）
# EXPOSE 8080

# 运行应用
CMD ["/app/rustcta"]
EOF

# 3. 构建生产镜像
docker build -f Dockerfile.prod -t rustcta:latest .

# 4. 运行生产容器
docker run -d --name rustcta-prod \
  --restart unless-stopped \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/log:/app/log \
  rustcta:latest
```

#### Docker Compose 部署（推荐）

创建 `docker-compose.yml`：

```yaml
version: '3.8'

services:
  rustcta:
    build: .
    container_name: rustcta
    restart: unless-stopped
    volumes:
      - ./config:/app/config:ro
      - ./log:/app/log
    environment:
      - RUST_LOG=info
    # ports:
    #   - "8080:8080"  # 如果需要暴露端口
    networks:
      - rustcta-network

networks:
  rustcta-network:
    driver: bridge

volumes:
  log-data:
```

使用 Docker Compose：

```bash
# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down

# 重启服务
docker-compose restart
```

## 配置说明

### 主配置文件 (config/config.json)

```json
{
  "strategies": [
    {
      "name": "grid_btcusdt",
      "strategy_type": "grid",
      "symbol": "BTCUSDT",
      "enabled": true,
      "grid_spacing": 0.001,
      "grid_levels": 10,
      "base_quantity": 0.001
    }
  ],
  "exchange": "binance",
  "log_level": "info"
}
```

### API 密钥配置 (config/api_keys.json)

```json
{
  "binance": {
    "api_key": "your_binance_api_key",
    "secret_key": "your_binance_secret_key"
  }
}
```

## 日志系统

系统使用 `log4rs` 进行日志管理：

- **时间精度**: 毫秒级时间戳
- **输出位置**: 同时输出到控制台和 `log/app.log` 文件
- **日志级别**: INFO, WARN, ERROR
- **成交日志**: 简化格式，只记录关键信息（交易对、方向、价格）

## 开发

### 代码格式化

```bash
# 格式化代码
cargo fmt

# 代码检查
cargo clippy

# 运行测试
cargo test
```

### 项目结构

```
src/
├── main.rs              # 程序入口
├── config/              # 配置管理
├── exchange/            # 交易所接口
├── strategy/            # 交易策略
├── utils/               # 工具函数
└── logger.rs            # 日志配置
```

## 安全注意事项

- 🔐 **API 密钥安全**: 不要将 API 密钥提交到版本控制
- 🌐 **网络安全**: 确保服务器防火墙配置正确
- 💰 **资金管理**: 建议先在测试网环境测试
- 📊 **监控告警**: 设置适当的监控和告警机制

## 故障排除

### 常见问题

#### 编译相关问题

1. **Docker 编译失败**
   ```bash
   # 检查 Docker 是否正常运行
   docker --version
   docker info
   
   # 清理 Docker 缓存重新编译
   docker system prune -f
   docker build --no-cache -t rustcta-builder .
   ```

2. **网络连接问题**
   ```bash
   # 如果在国内，可能需要使用镜像加速
   # 在 Dockerfile 中添加：
   # RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list
   ```

3. **权限错误**: `chmod +x ./rustcta-linux`

#### 运行时问题

4. **依赖缺失**: 安装 OpenSSL 开发库
   ```bash
   # Ubuntu/Debian
   sudo apt-get install libssl-dev
   
   # CentOS/RHEL
   sudo yum install openssl-devel
   ```

5. **网络连接**: 检查防火墙和网络配置
   ```bash
   # 测试币安 API 连接
   curl -I https://api.binance.com/api/v3/ping
   ```

6. **API 限制**: 检查 API 密钥权限和频率限制

7. **Docker 容器问题**
   ```bash
   # 查看容器日志
   docker logs rustcta
   
   # 进入容器调试
   docker exec -it rustcta /bin/bash
   
   # 重启容器
   docker restart rustcta
   ```

### 日志查看

```bash
# 查看实时日志
tail -f log/app.log

# 查看错误日志
grep ERROR log/app.log

# 查看成交日志
grep "成交" log/app.log
```

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License

## 联系方式

如有问题，请提交 Issue 或联系开发团队。