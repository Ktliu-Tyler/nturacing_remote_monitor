"""
NTURT CAN Data Client - Vehicle Side
====================================
This script runs on the vehicle's Raspberry Pi.
It reads CAN data from can0 and can1 and sends it to the remote server.
"""

import can
import asyncio
import websockets
import json
import time
import struct
from datetime import datetime
from packaging import version

# Configuration
SERVER_URL = "ws://140.112.16.226:8889"  # 修改为你的服务器地址
RECONNECT_DELAY = 5  # 重连延迟（秒）
HEARTBEAT_INTERVAL = 1  # 心跳间隔（秒）

class CANDataClient:
    def __init__(self, server_url):
        self.server_url = server_url
        self.websocket = None
        self.running = True
        self.bus0 = None
        self.bus1 = None
        self.message_count = 0
        self.last_heartbeat = time.time()
        
    async def connect(self):
        """连接到服务器"""
        while self.running:
            try:
                print(f"Connecting to server at {self.server_url}...")
                self.websocket = await websockets.connect(
                    self.server_url,
                    ping_interval=20,
                    ping_timeout=10
                )
                print("Connected to server successfully!")
                return True
            except Exception as e:
                print(f"Failed to connect to server: {e}")
                print(f"Retrying in {RECONNECT_DELAY} seconds...")
                await asyncio.sleep(RECONNECT_DELAY)
        return False
    
    def init_can_buses(self):
        """初始化CAN总线"""
        try:
            # 初始化 can0
            can_kwargs = dict(channel='can0')
            if version.parse(can.__version__) >= version.parse('4.2.0'):
                can_kwargs['interface'] = 'socketcan'
            else:
                can_kwargs['bustype'] = 'socketcan'
            self.bus0 = can.interface.Bus(**can_kwargs)
            print("CAN0 initialized successfully")
        except Exception as e:
            print(f"Warning: Could not initialize CAN0 bus: {e}")
            self.bus0 = None
        
        try:
            # 初始化 can1
            can1_kwargs = dict(channel='can1')
            if version.parse(can.__version__) >= version.parse('4.2.0'):
                can1_kwargs['interface'] = 'socketcan'
            else:
                can1_kwargs['bustype'] = 'socketcan'
            self.bus1 = can.interface.Bus(**can1_kwargs)
            print("CAN1 initialized successfully")
        except Exception as e:
            print(f"Warning: Could not initialize CAN1 bus: {e}")
            self.bus1 = None
    
    async def send_can_message(self, can_id, data, bus_id):
        """发送CAN消息到服务器"""
        if not self.websocket:
            return False
        
        try:
            message_data = {
                'type': 'can_message',
                'bus_id': bus_id,  # 0 for can0, 1 for can1
                'can_id': can_id,
                'data': list(data),  # 转换bytes为list
                'timestamp': time.time()
            }
            await self.websocket.send(json.dumps(message_data))
            self.message_count += 1
            return True
        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.WebSocketException) as e:
            # Connection closed, stop trying to send
            return False
        except Exception as e:
            print(f"Error sending message: {e}")
            return False
    
    async def send_heartbeat(self):
        """发送心跳包"""
        if not self.websocket:
            return
        
        try:
            heartbeat_data = {
                'type': 'heartbeat',
                'timestamp': time.time(),
                'message_count': self.message_count
            }
            await self.websocket.send(json.dumps(heartbeat_data))
            self.last_heartbeat = time.time()
        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.WebSocketException):
            # Connection closed, stop trying to send
            pass
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
    
    async def read_can0(self):
        """读取CAN0数据"""
        while self.running:
            try:
                if self.bus0 and self.websocket:
                    message = self.bus0.recv(timeout=0.001)
                    if message:
                        success = await self.send_can_message(
                            message.arbitration_id,
                            message.data,
                            bus_id=0
                        )
                        if not success:
                            # Connection lost, exit loop
                            break
                await asyncio.sleep(0.0001)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error reading CAN0: {e}")
                await asyncio.sleep(0.1)
    
    async def read_can1(self):
        """读取CAN1数据"""
        while self.running:
            try:
                if self.bus1 and self.websocket:
                    message = self.bus1.recv(timeout=0.001)
                    if message:
                        success = await self.send_can_message(
                            message.arbitration_id,
                            message.data,
                            bus_id=1
                        )
                        if not success:
                            # Connection lost, exit loop
                            break
                await asyncio.sleep(0.0001)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error reading CAN1: {e}")
                await asyncio.sleep(0.1)
    
    async def heartbeat_loop(self):
        """心跳循环"""
        while self.running:
            try:
                if self.websocket:
                    await self.send_heartbeat()
                else:
                    # Connection lost, exit loop
                    break
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
    
    async def run(self):
        """主运行循环"""
        print("Starting CAN Data Client...")
        
        # 初始化CAN总线
        self.init_can_buses()
        
        while self.running:
            # 连接到服务器
            if await self.connect():
                try:
                    # 创建并运行所有任务
                    tasks = [
                        asyncio.create_task(self.read_can0()),
                        asyncio.create_task(self.read_can1()),
                        asyncio.create_task(self.heartbeat_loop())
                    ]
                    
                    # 等待任务完成或连接断开
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # 取消剩余任务
                    for task in pending:
                        task.cancel()
                    
                    # 等待任务清理完成
                    await asyncio.gather(*pending, return_exceptions=True)
                    
                except websockets.exceptions.ConnectionClosed:
                    print("Connection to server closed")
                except Exception as e:
                    print(f"Error in main loop: {e}")
                finally:
                    # 确保所有任务都被取消
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    
                    # 关闭websocket
                    if self.websocket:
                        try:
                            await self.websocket.close()
                        except Exception:
                            pass
                    self.websocket = None
                    
                    print(f"Reconnecting in {RECONNECT_DELAY} seconds...")
                    await asyncio.sleep(RECONNECT_DELAY)
    
    def shutdown(self):
        """关闭客户端"""
        print("Shutting down client...")
        self.running = False
        
        if self.bus0:
            self.bus0.shutdown()
        if self.bus1:
            self.bus1.shutdown()

async def main():
    """主函数"""
    client = CANDataClient(SERVER_URL)
    
    try:
        await client.run()
    except KeyboardInterrupt:
        print("\nReceived shutdown signal")
    finally:
        client.shutdown()
        print("Client stopped")

if __name__ == "__main__":
    print("=" * 60)
    print("NTURT CAN Data Client - Vehicle Side")
    print("=" * 60)
    print(f"Server URL: {SERVER_URL}")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    asyncio.run(main())
