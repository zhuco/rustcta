# Rust CTA 交易系统 - Linux 部署指南

## 部署包内容

```
linux-deploy/
├── rustcta-linux          # 可执行文件 (Linux x86_64)
├── config/                # 配置文件目录
│   ├── api.yml           # API配置
│   ├── api.yml.example   # API配置示例
│   └── strategy.yml      # 策略配置
├── run.sh                # 启动脚本
├── rustcta.service       # systemd 服务文件
└── DEPLOY.md            # 本部署说明
```

## 快速部署

### 1. 上传文件到服务器

```bash
# 使用 scp 上传部署包
scp -r linux-deploy/ user@your-server:/tmp/

# 或使用 rsync
rsync -avz linux-deploy/ user@your-server:/tmp/rustcta/
```

### 2. 服务器端安装

```bash
# 登录服务器
ssh user@your-server

# 创建应用目录
sudo mkdir -p /opt/rustcta
sudo cp -r /tmp/linux-deploy/* /opt/rustcta/

# 设置权限
sudo chmod +x /opt/rustcta/rustcta-linux
sudo chmod +x /opt/rustcta/run.sh

# 创建用户和组 (可选，推荐)
sudo useradd -r -s /bin/false rustcta
sudo chown -R rustcta:rustcta /opt/rustcta
```

### 3. 配置文件设置

```bash
# 编辑API配置
sudo nano /opt/rustcta/config/api.yml

# 编辑策略配置
sudo nano /opt/rustcta/config/strategy.yml
```

## 运行方式

### 方式一：直接运行

```bash
cd /opt/rustcta
./rustcta-linux
```

### 方式二：使用启动脚本

```bash
cd /opt/rustcta

# 启动
./run.sh start

# 停止
./run.sh stop

# 重启
./run.sh restart

# 查看状态
./run.sh status
```

### 方式三：systemd 服务 (推荐)

```bash
# 安装服务文件
sudo cp /opt/rustcta/rustcta.service /etc/systemd/system/

# 重新加载 systemd
sudo systemctl daemon-reload

# 启用服务 (开机自启)
sudo systemctl enable rustcta

# 启动服务
sudo systemctl start rustcta

# 查看状态
sudo systemctl status rustcta

# 查看日志
sudo journalctl -u rustcta -f
```

## 监控和维护

### 查看日志

```bash
# 应用日志 (使用启动脚本时)
tail -f /opt/rustcta/logs/rustcta.log

# 系统日志 (使用 systemd 时)
sudo journalctl -u rustcta -f
```

### 性能监控

```bash
# 查看进程状态
ps aux | grep rustcta

# 查看资源使用
top -p $(pgrep rustcta)

# 查看网络连接
netstat -tulpn | grep rustcta
```

### 更新部署

```bash
# 停止服务
sudo systemctl stop rustcta
# 或
./run.sh stop

# 备份当前版本
sudo cp /opt/rustcta/rustcta-linux /opt/rustcta/rustcta-linux.backup

# 替换新版本
sudo cp new-rustcta-linux /opt/rustcta/rustcta-linux
sudo chmod +x /opt/rustcta/rustcta-linux

# 重启服务
sudo systemctl start rustcta
# 或
./run.sh start
```

## 故障排除

### 常见问题

1. **权限错误**
   ```bash
   sudo chmod +x /opt/rustcta/rustcta-linux
   ```

2. **配置文件错误**
   ```bash
   # 检查配置文件格式
   /opt/rustcta/rustcta-linux --check-config
   ```

3. **端口占用**
   ```bash
   # 检查端口使用情况
   sudo netstat -tulpn | grep :8080
   ```

4. **依赖库缺失**
   ```bash
   # 检查依赖
   ldd /opt/rustcta/rustcta-linux
   ```

### 系统要求

- **操作系统**: Linux x86_64 (Ubuntu 18.04+, CentOS 7+, Debian 9+)
- **内存**: 最小 512MB，推荐 1GB+
- **磁盘**: 最小 100MB 可用空间
- **网络**: 需要访问币安API (api.binance.com)

### 安全建议

1. 使用专用用户运行应用
2. 配置防火墙规则
3. 定期备份配置文件
4. 监控日志异常
5. 使用 HTTPS 代理 (如需要)

## 联系支持

如遇到问题，请检查：
1. 应用日志文件
2. 系统日志
3. 网络连接状态
4. 配置文件格式

---

**注意**: 请确保在生产环境中妥善保管API密钥，不要在日志中记录敏感信息。