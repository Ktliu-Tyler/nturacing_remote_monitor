#!/bin/bash
# 安装 NTURT Server 服务的脚本

echo "正在安装 NTURT Server 服务..."

# 给启动脚本添加执行权限
chmod +x /home/pi/Desktop/GUI_SC-dev/start_server.sh

# 复制 service 文件到 systemd 目录
sudo cp /home/pi/Desktop/GUI_SC-dev/nturt-server.service /etc/systemd/system/

# 重新加载 systemd
sudo systemctl daemon-reload

# 启用服务（开机自启动）
sudo systemctl enable nturt-server.service

# 启动服务
sudo systemctl start nturt-server.service

echo ""
echo "安装完成！"
echo ""
echo "常用命令："
echo "  查看状态: sudo systemctl status nturt-server"
echo "  启动服务: sudo systemctl start nturt-server"
echo "  停止服务: sudo systemctl stop nturt-server"
echo "  重启服务: sudo systemctl restart nturt-server"
echo "  查看日志: sudo journalctl -u nturt-server -f"
echo "  禁用开机启动: sudo systemctl disable nturt-server"
