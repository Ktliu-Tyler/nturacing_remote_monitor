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
import os
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
LOGS_DIR = "../LOGS"  # CSV文件目录
CSV_REPLAY_SPEED = 1.0  # CSV回放速度倍数

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
        
        # 去重统计
        self.total_can_received = 0  # 从CAN收到的总消息数
        self.filtered_duplicates = 0  # 被过滤的重复消息数
        
        # 最新消息缓存 - 用于去重
        # key: (bus_id, can_id), value: message_data
        self.latest_messages = {}
        self.cache_lock = asyncio.Lock()
        
        # CSV回放模式
        self.mode = 'realtime'  # 'realtime' 或 'csv'
        self.csv_file = None
        self.csv_paused = False
        
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
    
    def scan_csv_files(self):
        """扫描LOGS目录下的CSV文件"""
        import os
        import glob
        
        try:
            csv_files = []
            pattern = os.path.join(LOGS_DIR, '*.csv')
            
            for filepath in glob.glob(pattern):
                filename = os.path.basename(filepath)
                file_stat = os.stat(filepath)
                file_time = datetime.fromtimestamp(file_stat.st_mtime)
                file_size = file_stat.st_size
                
                csv_files.append({
                    'filename': filename,
                    'path': filepath,
                    'modified': file_time.isoformat(),
                    'size': file_size,
                    'size_mb': round(file_size / 1024 / 1024, 2)
                })
            
            # 按修改时间倒序排序
            csv_files.sort(key=lambda x: x['modified'], reverse=True)
            return csv_files
        except Exception as e:
            print(f"Error scanning CSV files: {e}")
            return []
    
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
        self.total_can_received += 1
        
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
        
        # 批量模式 - 智能去重
        should_queue = False
        async with self.cache_lock:
            # 检查是否是新数据
            if message_key in self.latest_messages:
                old_data = self.latest_messages[message_key]['data']
                # 只有数据真正变化时才加入队列
                if old_data != data_list:
                    should_queue = True
                    self.latest_messages[message_key] = message_data
                # 数据相同则只更新时间戳，不重复加入队列（这就是去重！）
                else:
                    self.filtered_duplicates += 1
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
            filter_rate = 0
            if self.total_can_received > 0:
                filter_rate = (self.filtered_duplicates / self.total_can_received) * 100
            
            heartbeat_data = {
                'type': 'heartbeat',
                'timestamp': time.time(),
                'message_count': self.message_count,
                'dropped_messages': self.dropped_messages,
                'queue_size': self.message_queue.qsize(),
                'total_received': self.total_can_received,
                'filtered': self.filtered_duplicates,
                'filter_rate': f"{filter_rate:.1f}%",
                'mode': self.mode,
                'csv_file': os.path.basename(self.csv_file) if self.csv_file else None,
                'csv_paused': self.csv_paused
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
            batch = []
            last_send_time = time.time()
            
            # 等待websocket连接建立
            while self.running and not self.websocket:
                await asyncio.sleep(0.1)
            
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
                                    filter_rate = 0
                                    if self.total_can_received > 0:
                                        filter_rate = (self.filtered_duplicates / self.total_can_received) * 100
                                    print(f"📊 Sent: {self.message_count} msgs | Received: {self.total_can_received} | Filtered: {filter_rate:.1f}% | Queue: {self.message_queue.qsize()}")
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
    
    async def csv_replayer(self):
        """从CSV文件回放CAN数据"""
        import csv
        
        print(f"Starting CSV replay from: {self.csv_file}")
        
        try:
            with open(self.csv_file, 'r') as f:
                reader = csv.DictReader(f)
                
                last_timestamp = None
                start_time = time.time()
                
                for row in reader:
                    if not self.running or self.mode != 'csv':
                        print("CSV replay stopped")
                        break
                    
                    # 暂停功能
                    while self.csv_paused and self.running:
                        await asyncio.sleep(0.1)
                    
                    try:
                        # 解析CSV数据
                        timestamp = float(row.get('timestamp', 0))
                        can_id = int(row.get('id', row.get('can_id', '0')), 16 if 'x' in str(row.get('id', row.get('can_id', '0'))) else 10)
                        bus_id = int(row.get('bus', row.get('bus_id', 0)))
                        
                        # 解析data字段
                        data_str = row.get('data', '')
                        if data_str:
                            # 处理不同格式："01 02 03" 或 "[1,2,3]" 或 "010203"
                            data_str = data_str.strip('[]').replace(',', ' ')
                            data_bytes = bytes([int(x, 16) for x in data_str.split()])
                        else:
                            data_bytes = bytes()
                        
                        # 时间同步回放
                        if last_timestamp is not None:
                            time_diff = (timestamp - last_timestamp) / CSV_REPLAY_SPEED
                            if time_diff > 0:
                                await asyncio.sleep(time_diff)
                        
                        last_timestamp = timestamp
                        
                        # 发送CAN消息
                        await self.send_can_message(can_id, data_bytes, bus_id)
                        
                    except Exception as e:
                        print(f"Error parsing CSV row: {e}, row: {row}")
                        continue
                
                print("CSV replay completed")
                # 回放完成后切换回实时模式
                self.mode = 'realtime'
                
                # 通知服务器回放完成
                if self.websocket:
                    await self.websocket.send(json.dumps({
                        'type': 'csv_status',
                        'status': 'completed',
                        'message': 'CSV replay completed'
                    }))
                
        except Exception as e:
            print(f"Error in CSV replayer: {e}")
            import traceback
            traceback.print_exc()
            self.mode = 'realtime'
    
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
    
    async def command_receiver(self):
        """接收并处理服务器命令"""
        while self.running:
            try:
                if not self.websocket:
                    await asyncio.sleep(0.1)
                    continue
                
                # 接收服务器消息
                message = await self.websocket.recv()
                data = json.loads(message)
                
                cmd_type = data.get('type')
                
                if cmd_type == 'request_csv_list':
                    # 服务器请求CSV文件列表
                    print("Received request for CSV file list")
                    csv_files = self.scan_csv_files()
                    
                    response = {
                        'type': 'csv_list',
                        'files': csv_files,
                        'count': len(csv_files)
                    }
                    await self.websocket.send(json.dumps(response))
                    print(f"Sent {len(csv_files)} CSV files to server")
                
                elif cmd_type == 'select_csv':
                    # 服务器选择了CSV文件
                    filename = data.get('filename')
                    print(f"Received CSV selection: {filename}")
                    
                    # 切换到CSV模式
                    import os
                    self.csv_file = os.path.join(LOGS_DIR, filename)
                    
                    if os.path.exists(self.csv_file):
                        self.mode = 'csv'
                        print(f"Switched to CSV mode: {self.csv_file}")
                        
                        # 确认切换
                        await self.websocket.send(json.dumps({
                            'type': 'mode_changed',
                            'mode': 'csv',
                            'file': filename
                        }))
                    else:
                        print(f"CSV file not found: {self.csv_file}")
                        await self.websocket.send(json.dumps({
                            'type': 'error',
                            'message': f'File not found: {filename}'
                        }))
                
                elif cmd_type == 'switch_realtime':
                    # 切换回实时模式
                    print("Switching back to realtime mode")
                    self.mode = 'realtime'
                    self.csv_file = None
                    
                    await self.websocket.send(json.dumps({
                        'type': 'mode_changed',
                        'mode': 'realtime'
                    }))
                
                elif cmd_type == 'csv_pause':
                    # 暂停/恢复CSV回放
                    self.csv_paused = not self.csv_paused
                    print(f"CSV replay {'paused' if self.csv_paused else 'resumed'}")
                    
            except websockets.exceptions.ConnectionClosed:
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in command receiver: {e}")
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
                        asyncio.create_task(self.batch_sender()),
                        asyncio.create_task(self.heartbeat_loop()),
                        asyncio.create_task(self.command_receiver())
                    ]
                    
                    # 根据模式添加数据源任务
                    if self.mode == 'realtime':
                        tasks.append(asyncio.create_task(self.read_can0()))
                        tasks.append(asyncio.create_task(self.read_can1()))
                    elif self.mode == 'csv' and self.csv_file:
                        tasks.append(asyncio.create_task(self.csv_replayer()))
                    
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
