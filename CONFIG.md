# NTURT CAN Monitor - Client/Server Configuration
# ================================================

# Client Configuration (车辆端配置文件)
# 在车辆的RPI上修改 client_vehicle.py 中的以下参数：

## 服务器地址
SERVER_URL = "ws://YOUR_SERVER_IP:8889"
# 例如: "ws://100.127.237.75:8889" 或 "ws://192.168.1.100:8889"

## 重连延迟（秒）
RECONNECT_DELAY = 5

## 心跳间隔（秒）
HEARTBEAT_INTERVAL = 1

# Server Configuration (服务器端配置文件)
# 在远端RPI上修改 server_remote.py 中的以下参数：

## 网页服务端口
WEB_PORT = 8888

## 接收车辆数据的端口
DATA_PORT = 8889

## 日志目录
DIRBASE = "../LOGS/"

# ================================================
# 部署说明
# ================================================

## 1. 车辆端 (Vehicle RPI)
# 
# 步骤：
# 1. 将 client_vehicle.py 复制到车辆的RPI
# 2. 修改 SERVER_URL 为你的服务器IP地址
# 3. 确保CAN接口已配置 (can0, can1)
# 4. 运行: python3 client_vehicle.py
#
# 依赖包：
# - python-can
# - websockets
# - asyncio

## 2. 服务器端 (Remote RPI)
#
# 步骤：
# 1. 将 server_remote.py 复制到远端RPI
# 2. 确保 templates/ 和 static/ 目录存在
# 3. 运行: python3 server_remote.py
# 4. 在浏览器访问: http://SERVER_IP:8888
#
# 依赖包：
# - fastapi
# - uvicorn
# - websockets
# - jinja2
# - python-multipart

## 3. 网络配置
#
# - 确保车辆RPI和服务器RPI在同一网络或可以互相访问
# - 如果使用Tailscale，可以使用Tailscale IP地址
# - 确保防火墙允许端口 8888 (网页) 和 8889 (数据)

## 4. 自动启动 (可选)
#
# 使用systemd服务自动启动：
#
# 车辆端: /etc/systemd/system/can-client.service
# [Unit]
# Description=CAN Data Client
# After=network.target
#
# [Service]
# Type=simple
# User=pi
# WorkingDirectory=/home/pi/GUI-dev
# ExecStart=/usr/bin/python3 /home/pi/GUI-dev/client_vehicle.py
# Restart=always
#
# [Install]
# WantedBy=multi-user.target
#
# 服务器端: /etc/systemd/system/can-server.service
# [Unit]
# Description=CAN Data Server
# After=network.target
#
# [Service]
# Type=simple
# User=pi
# WorkingDirectory=/home/pi/GUI-dev
# ExecStart=/usr/bin/python3 /home/pi/GUI-dev/server_remote.py
# Restart=always
#
# [Install]
# WantedBy=multi-user.target
#
# 启用服务：
# sudo systemctl enable can-client  # 车辆端
# sudo systemctl enable can-server  # 服务器端
# sudo systemctl start can-client
# sudo systemctl start can-server

## 5. 测试连接
#
# 1. 先启动服务器端
# 2. 再启动车辆端
# 3. 查看车辆端输出是否显示 "Connected to server successfully!"
# 4. 在浏览器访问服务器网页，查看是否有数据更新
# 5. 访问 http://SERVER_IP:8888/api/status 查看连接状态

# ================================================
# 故障排除
# ================================================

## 连接问题
# - 检查网络连接
# - 检查防火墙设置
# - 确认SERVER_URL配置正确
# - 查看服务器日志

## 数据不更新
# - 确认CAN接口正常工作
# - 检查车辆端是否正常发送数据
# - 查看服务器端是否接收到数据
# - 检查 /api/status 中的 message_count

## 性能优化
# - 调整 HEARTBEAT_INTERVAL 减少心跳频率
# - 在服务器端调整 broadcaster_loop 的 sleep 时间
# - 检查网络带宽和延迟
