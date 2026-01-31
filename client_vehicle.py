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
BATCH_SIZE = 50  # 批量发送的消息数量（增大以提高效率）
BATCH_TIMEOUT = 0.05  # 批量发送超时（秒），50ms
MAX_QUEUE_SIZE = 1000  # 最大队列大小，防止内存溢出
USE_BATCH_MODE = True  # 批量发送模式

class CANDataClient:
    def __init__(self, server_url):
        self.server_url = server_url
        self.websocket = None
        self.running = True
        self.bus0 = None
        self.bus1 = None
        self.message_count = 0
        self.last_heartbeat = time.time()
        
        # 批量发送队列
        self.message_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.dropped_messages = 0
        self.sent_batches = 0
        self.last_send_report = time.time()
        
        # 最新消息缓存 - 用于去重
        # key: (bus_id, can_id), value: message_data
        self.latest_messages = {}
        self.cache_lock = asyncio.Lock()
        
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
        """将CAN消息加入队列（智能策略：数据相同去重，不同则保留）"""
        message_key = (bus_id, can_id)
        data_list = list(data)
        message_data = {
            'bus_id': bus_id,
            'can_id': can_id,
            'data': data_list,
            'timestamp': time.time()
        }
        
        # 如果使用单条发送模式（用于测试）
        if not USE_BATCH_MODE:
            if self.websocket:
                try:
                    single_message = {
                        'type': 'can_message',
                        'bus_id': bus_id,
                        'can_id': can_id,
                        'data': data_list,
                        'timestamp': time.time()
                    }
                    await self.websocket.send(json.dumps(single_message))
                    self.message_count += 1
                    return True
                except Exception as e:
                    return False
            return False
        
        # 批量模式（原逻辑）
        should_queue = False
        async with self.cache_lock:
            # 检查是否是新数据
            if message_key in self.latest_messages:
                old_data = self.latest_messages[message_key]['data']
                # 只有数据真正变化时才加入队列
                if old_data != data_list:
                    should_queue = True
                    self.latest_messages[message_key] = message_data
                # 数据相同则只更新时间戳，不重复加入队列
                else:
                    self.latest_messages[message_key]['timestamp'] = message_data['timestamp']
            else:
                # 新的CAN ID，加入队列
                should_queue = True
                self.latest_messages[message_key] = message_data
        
        if should_queue:
            try:
                # 直接把完整消息放入队列，不只是key
                self.message_queue.put_nowait(message_data)
                return True
            except asyncio.QueueFull:
                # 队列满时已经在缓存中更新了最新值
                self.dropped_messages += 1
                if self.dropped_messages % 100 == 0:
                    print(f"Warning: Queue full ({self.message_queue.qsize()}), data cached. Delayed: {self.dropped_messages}")
                return False
        return True
    
    async def send_heartbeat(self):
        """发送心跳包"""
        if not self.websocket:
            return
        
        try:
            heartbeat_data = {
                'type': 'heartbeat',
                'timestamp': time.time(),
                'message_count': self.message_count,
                'dropped_messages': self.dropped_messages,
                'queue_size': self.message_queue.qsize()
            }
            await self.websocket.send(json.dumps(heartbeat_data))
            self.last_heartbeat = time.time()
        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.WebSocketException):
            # Connection closed, stop trying to send
            pass
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
    
    async def batch_sender(self):
        """批量发送消息到服务器（保留所有数据变化）"""
        try:
            print("=== Batch sender task starting ===", flush=True)
            batch = []
            last_send_time = time.time()
            
            # 等待websocket连接建立
            while self.running and not self.websocket:
                print("Batch sender: Waiting for websocket connection...", flush=True)
                await asyncio.sleep(0.1)
            
            print("Batch sender: Active and ready to send", flush=True)
            
            # 主发送循环
            while self.running:
                try:
                    if not self.websocket:
                        await asyncio.sleep(0.1)
                        continue
                    
                    try:
                        # 尝试从队列获取消息，超时时间为BATCH_TIMEOUT
                        timeout = max(0.001, BATCH_TIMEOUT - (time.time() - last_send_time))
                        message = await asyncio.wait_for(
                            self.message_queue.get(),
                            timeout=timeout
                        )
                        batch.append(message)
                        
                        # 如果达到批量大小或超时，发送批量数据
                        current_time = time.time()
                        should_send = (
                            len(batch) >= BATCH_SIZE or
                            (batch and current_time - last_send_time >= BATCH_TIMEOUT)
                        )
                        
                        if should_send:
                            # 批量发送
                            batch_data = {
                                'type': 'can_batch',
                                'messages': batch,
                                'count': len(batch)
                            }
                            try:
                                await self.websocket.send(json.dumps(batch_data))
                                self.message_count += len(batch)
                                self.sent_batches += 1
                                
                                # 定期报告发送状态
                                if time.time() - self.last_send_report > 5:
                                    print(f"Sent {self.sent_batches} batches, {self.message_count} messages, Queue: {self.message_queue.qsize()}")
                                    self.last_send_report = time.time()
                            except Exception as send_err:
                                print(f"Error sending batch: {send_err}")
                                raise
                            
                            batch = []
                            last_send_time = current_time
                            
                    except asyncio.TimeoutError:
                        # 超时，如果有数据就发送
                        if batch:
                            batch_data = {
                                'type': 'can_batch',
                                'messages': batch,
                                'count': len(batch)
                            }
                            try:
                                await self.websocket.send(json.dumps(batch_data))
                                self.message_count += len(batch)
                                self.sent_batches += 1
                            except Exception as send_err:
                                print(f"Error sending batch (timeout): {send_err}")
                                raise
                            
                            batch = []
                            last_send_time = time.time()
                            
                except (websockets.exceptions.ConnectionClosed, websockets.exceptions.WebSocketException):
                    # 连接关闭，清空批次
                    batch = []
                    break
                except asyncio.CancelledError:
                    print("Batch sender: Task cancelled", flush=True)
                    break
                except Exception as e:
                    print(f"Error in batch sender: {e}", flush=True)
                    import traceback
                    traceback.print_exc()
                    await asyncio.sleep(0.1)
        except Exception as outer_e:
            print(f"FATAL error in batch_sender: {outer_e}", flush=True)
            import traceback
            traceback.print_exc()
    
    async def read_can0(self):
        """读取CAN0数据"""
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                if self.bus0 and self.websocket:
                    # 使用线程池执行阻塞的recv调用
                    message = await loop.run_in_executor(
                        None,  # 使用默认线程池
                        lambda: self.bus0.recv(timeout=0.01)
                    )
                    if message:
                        await self.send_can_message(
                            message.arbitration_id,
                            message.data,
                            bus_id=0
                        )
                else:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error reading CAN0: {e}")
                await asyncio.sleep(0.1)
    
    async def read_can1(self):
        """读取CAN1数据"""
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                if self.bus1 and self.websocket:
                    # 使用线程池执行阻塞的recv调用
                    message = await loop.run_in_executor(
                        None,  # 使用默认线程池
                        lambda: self.bus1.recv(timeout=0.01)
                    )
                    if message:
                        await self.send_can_message(
                            message.arbitration_id,
                            message.data,
                            bus_id=1
                        )
                else:
                    await asyncio.sleep(0.01)
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
                    print("Creating tasks...", flush=True)
                    task_can0 = asyncio.create_task(self.read_can0())
                    print("CAN0 task created", flush=True)
                    task_can1 = asyncio.create_task(self.read_can1())
                    print("CAN1 task created", flush=True)
                    task_sender = asyncio.create_task(self.batch_sender())
                    print("Batch sender task created", flush=True)
                    task_heartbeat = asyncio.create_task(self.heartbeat_loop())
                    print("Heartbeat task created", flush=True)
                    
                    tasks = [task_can0, task_can1, task_sender, task_heartbeat]
                    print(f"All tasks created and added to list", flush=True)
                    
                    # 短暂延迟让任务启动
                    await asyncio.sleep(0.1)
                    print(f"After 0.1s delay, checking task states...", flush=True)
                    for i, task in enumerate(tasks):
                        if task.done():
                            print(f"Task {i} already done! Exception: {task.exception()}", flush=True)
                    
                    # 等待任务完成或连接断开
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # 检查完成的任务是否有异常
                    for task in done:
                        if task.exception():
                            print(f"Task failed with exception: {task.exception()}")
                    
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
