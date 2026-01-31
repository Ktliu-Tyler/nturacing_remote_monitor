"""
NTURT CAN Data Server - Remote Side
===================================
This script runs on the remote Raspberry Pi.
It receives CAN data from vehicle clients and serves web dashboard to users.
"""

import can
import struct
import asyncio
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from datetime import datetime
import time
import json
import threading
from typing import List, Dict
import csv
import os
import websockets

# Configuration
WEB_PORT = 8888  # 网页服务端口
DATA_PORT = 8889  # 接收车辆数据的端口
DIRBASE = "../LOGS/"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# WebSocket connections for web clients
web_connections: List[WebSocket] = []

# Vehicle client connections
vehicle_clients: Dict[str, dict] = {}

class RemoteCANServer:
    def __init__(self):
        # Connection status
        self.vehicle_connected = False
        self.last_data_time = None
        self.connection_timeout = 3.0  # 3秒没有数据则判定断线
        
        # Data storage (相同的数据结构)
        self.data_store = {
            'timestamp': {'time': None, 'last_update': None},
            'gps': {
                'lat': None, 'lon': None, 'alt': None, 'status': None,
                'last_update': None
            },
            'covariance': {
                'values': [0.0] * 9, 'type': 0, 'type_name': 'UNKNOWN',
                'last_update': None
            },
            'velocity': {
                'linear_x': None, 'linear_y': None, 'linear_z': None,
                'angular_x': None, 'angular_y': None, 'angular_z': None,
                'magnitude': None, 'speed_kmh': None,
                'last_update': None
            },
            'accumulator': {
                'soc': None, 'voltage': None, 'current': None, 'temperature': None,
                'status': None, 'heartbeat': None, 'capacity': None,
                'cell_voltages': [None] * 105, 'cell_temperatures': [None] * 224,
                'last_update': None
            },
            'inverters': {
                1: {'name': 'FL', 'status': None, 'torque': None, 'speed': None,
                    'control_word': None, 'target_torque': None,
                    'dc_voltage': None, 'dc_current': None,
                    'mos_temp': None, 'mcu_temp': None, 'motor_temp': None,
                    'heartbeat': None, 'last_update': None},
                2: {'name': 'FR', 'status': None, 'torque': None, 'speed': None,
                    'control_word': None, 'target_torque': None,
                    'dc_voltage': None, 'dc_current': None,
                    'mos_temp': None, 'mcu_temp': None, 'motor_temp': None,
                    'heartbeat': None, 'last_update': None},
                3: {'name': 'RL', 'status': None, 'torque': None, 'speed': None,
                    'control_word': None, 'target_torque': None,
                    'dc_voltage': None, 'dc_current': None,
                    'mos_temp': None, 'mcu_temp': None, 'motor_temp': None,
                    'heartbeat': None, 'last_update': None},
                4: {'name': 'RR', 'status': None, 'torque': None, 'speed': None,
                    'control_word': None, 'target_torque': None,
                    'dc_voltage': None, 'dc_current': None,
                    'mos_temp': None, 'mcu_temp': None, 'motor_temp': None,
                    'heartbeat': None, 'last_update': None}
            },
            'vcu': {
                'steer': None, 'accel': None, 'apps1': None,    
                'apps2': None, 'brake': None, 'bse1': None, 'bse2': None,
                'suspF': None, 'suspR': None,
                'last_update': None},
            'imu': {
                'accel_km6': {'x': None, 'y': None, 'z': None},
                'accel_km308': {'x': None, 'y': None, 'z': None},
                'gyro': {'x': None, 'y': None, 'z': None},
                'euler': {'roll': None, 'pitch': None, 'yaw': None},
                'mag': {'x': None, 'y': None, 'z': None},
                'last_update': None
            },
            'imu2': {
                'accel': {'x': None, 'y': None, 'z': None},
                'gyro': {'x': None, 'y': None, 'z': None},
                'quaternion': {'w': None, 'x': None, 'y': None, 'z': None},
                'last_update': None
            },
            'distance': {
                'trip_distance_km': None,
                'last_update': None
            },
            'xsens': {
                'quaternion': {'q0': None, 'q1': None, 'q2': None, 'q3': None},
                'delta_v': {'x': None, 'y': None, 'z': None, 'exponent': None},
                'rate_of_turn': {'gyr_x': None, 'gyr_y': None, 'gyr_z': None},
                'delta_q': {'dq0': None, 'dq1': None, 'dq2': None, 'dq3': None},
                'acceleration': {'acc_x': None, 'acc_y': None, 'acc_z': None},
                'magnetic_field': {'mag_x': None, 'mag_y': None, 'mag_z': None},
                'gps': {'lat': None, 'lon': None, 'alt': None},
                'velocity': {'vel_x': None, 'vel_y': None, 'vel_z': None},
                'last_update': None
            }
        }
        
        self.message_count = 0
        self.running = True
        
        # Legacy GPS数据暂存
        self.gps_lat = None
        self.gps_lon = None
        self.gps_alt = None
        
        # Position covariance 暂存
        self.position_covariance = [0.0] * 9
        self.position_covariance_type = 0
        
        print("Remote CAN Server initialized")

    def check_vehicle_connection(self):
        """检查车辆连接状态"""
        if not vehicle_clients:
            self.vehicle_connected = False
            return False
        
        # 检查是否有最近的数据更新
        if self.last_data_time is not None:
            time_since_update = time.time() - self.last_data_time
            if time_since_update > self.connection_timeout:
                self.vehicle_connected = False
                return False
            else:
                self.vehicle_connected = True
                return True
        else:
            # 有客户端但还没有数据，暂时认为是连接的
            self.vehicle_connected = bool(vehicle_clients)
            return self.vehicle_connected

    async def handle_vehicle_client(self, websocket):
        """处理来自车辆的连接"""
        client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        print(f"Vehicle client connected: {client_id}")
        
        vehicle_clients[client_id] = {
            'websocket': websocket,
            'connected_at': time.time(),
            'last_heartbeat': time.time(),
            'message_count': 0
        }
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    
                    if data['type'] == 'can_message':
                        # 处理单条CAN消息（向后兼容）
                        can_id = data['can_id']
                        can_data = bytes(data['data'])
                        bus_id = data.get('bus_id', 0)
                        
                        # 创建模拟CAN消息对象
                        mock_message = self.create_mock_can_message(can_id, can_data)
                        self.process_can_message(mock_message)
                        self.message_count += 1
                        vehicle_clients[client_id]['message_count'] += 1
                        
                        # 更新最后数据接收时间
                        self.last_data_time = time.time()
                        self.vehicle_connected = True
                    
                    elif data['type'] == 'can_batch':
                        # 处理批量CAN消息
                        messages = data.get('messages', [])
                        if len(messages) > 0:
                            # 只在第一次收到时打印
                            if self.message_count == 0:
                                print(f"[DEBUG] First batch received: {len(messages)} messages")
                            
                            for msg in messages:
                                can_id = msg['can_id']
                                can_data = bytes(msg['data'])
                                bus_id = msg.get('bus_id', 0)
                                
                                # 创建模拟CAN消息对象
                                mock_message = self.create_mock_can_message(can_id, can_data)
                                self.process_can_message(mock_message)
                                self.message_count += 1
                                vehicle_clients[client_id]['message_count'] += 1
                            
                            # 更新最后数据接收时间
                            self.last_data_time = time.time()
                            self.vehicle_connected = True
                        
                    elif data['type'] == 'heartbeat':
                        # 更新心跳时间
                        vehicle_clients[client_id]['last_heartbeat'] = time.time()
                        
                        # 打印统计信息
                        if 'dropped_messages' in data and data['dropped_messages'] > 0:
                            print(f"Client {client_id}: Dropped {data['dropped_messages']} messages, Queue: {data.get('queue_size', 0)}")
                        
                except json.JSONDecodeError:
                    print(f"Invalid JSON from {client_id}")
                except Exception as e:
                    print(f"Error processing message from {client_id}: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            print(f"Vehicle client disconnected: {client_id}")
        finally:
            if client_id in vehicle_clients:
                del vehicle_clients[client_id]

    def create_mock_can_message(self, can_id, data):
        """创建模拟的CAN消息对象"""
        class MockCanMessage:
            def __init__(self, arbitration_id, data):
                self.arbitration_id = arbitration_id
                self.data = data
        
        return MockCanMessage(can_id, data)

    async def broadcaster_loop(self):
        """定期广播数据到网页客户端"""
        while self.running:
            if web_connections:
                await self.broadcast_data()
            await asyncio.sleep(0.05)  # 20 FPS

    async def broadcast_data(self):
        """广播数据到所有网页客户端"""
        if not web_connections:
            return
        
        # 检查连接状态
        self.check_vehicle_connection()
            
        broadcast_data = {
            'timestamp': self.data_store['timestamp']['time'].isoformat() if self.data_store['timestamp']['time'] else None,
            'gps': self.data_store['gps'],
            'velocity': self.data_store['velocity'],
            'distance': self.data_store['distance'],
            'accumulator': self.data_store['accumulator'],
            'inverters': self.data_store['inverters'],
            'vcu': self.data_store['vcu'],
            'imu': self.data_store['imu'],
            'imu2': self.data_store['imu2'],
            'xsens': self.data_store['xsens'],
            'message_count': self.message_count,
            'update_time': datetime.now().isoformat(),
            'vehicle_clients': len(vehicle_clients),
            'vehicle_connected': self.vehicle_connected,
            'last_data_time': self.last_data_time
        }
        
        # 发送到所有网页客户端
        disconnected = []
        for websocket in web_connections:
            try:
                await websocket.send_text(json.dumps(broadcast_data))
            except:
                disconnected.append(websocket)
        
        # 移除断开的连接
        for ws in disconnected:
            if ws in web_connections:
                web_connections.remove(ws)

    # 从这里开始，复制所有的decode函数
    def process_can_message(self, msg):
        """处理CAN消息 - 与原来的代码相同"""
        can_id = msg.arbitration_id
        data = msg.data
        
        try:
            # Timestamp 解码
            if can_id == 0x100:
                self.decode_timestamp(data)
            
            elif can_id == 0x181:
                self.decode_vcu_cockpit(data)
            elif can_id == 0x381:
                self.decode_vcu_suspension(data)
            # GPS 解码
            elif can_id == 0x400:
                self.decode_gps_basic(data)
            elif can_id == 0x401:
                self.decode_gps_extended(data)
            elif 0x410 <= can_id <= 0x418:
                self.decode_position_covariance(data, can_id - 0x410)
            elif can_id == 0x419:
                self.decode_position_covariance_type(data)
                
            # 速度资料解码
            elif can_id == 0x402:
                self.decode_velocity_x(data)
            elif can_id == 0x403:
                self.decode_velocity_y(data)
            elif can_id == 0x404:
                self.decode_velocity_z(data)
            elif can_id == 0x405:
                self.decode_angular_x(data)
            elif can_id == 0x406:
                self.decode_angular_y(data)
            elif can_id == 0x407:
                self.decode_angular_z(data)
            elif can_id == 0x408:
                self.decode_velocity_magnitude(data)
            elif can_id == 0x440:
                self.decode_distance(data)
            
            # Accumulator 解码
            elif can_id == 0x601:
                self.decode_cell_voltage(data)
            elif can_id == 0x651:
                self.decode_accumulator_temperature(data)
            elif can_id == 0x710:
                self.decode_accumulator_heartbeat(data)
            elif can_id == 0x501:
                self.decode_accumulator_status(data)
            elif can_id == 0x511:
                self.decode_accumulator_state(data)
            
            # Inverter 解码
            elif 0x191 <= can_id <= 0x194:
                inv_num = can_id - 0x190
                self.decode_inverter_status(data, inv_num)
            elif 0x291 <= can_id <= 0x294:
                inv_num = can_id - 0x290
                self.decode_inverter_state(data, inv_num)
            elif 0x391 <= can_id <= 0x394:
                inv_num = can_id - 0x390
                self.decode_inverter_temperature(data, inv_num)
            elif 0x711 <= can_id <= 0x714:
                inv_num = can_id - 0x710
                self.decode_inverter_heartbeat(data, inv_num)
            elif 0x210 <= can_id <= 0x214:
                inv_num = can_id - 0x210
                self.decode_inverter_control(data, inv_num)
            
            # IMU 解码
            elif can_id == 0x185:
                self.decode_imu_accel_km6(data)
            elif can_id == 0x426:
                self.decode_imu_accel_km308(data)
            elif can_id == 0x285:
                self.decode_imu_gyro(data)
            elif can_id == 0x385:
                self.decode_imu_euler(data)
            elif can_id == 0x429:
                self.decode_imu_mag(data)
            
            # IMU2 解码
            elif can_id == 0x188:
                self.decode_imu2_accel(data)
            elif can_id == 0x288:
                self.decode_imu2_gyro(data)
            elif can_id == 0x488:
                self.decode_imu2_quaternion(data)
            
            # Xsens IMU 解码
            elif can_id == 0x021:
                self.decode_xsens_quaternion(data)
            elif can_id == 0x031:
                self.decode_xsens_delta_v(data)
            elif can_id == 0x032:
                self.decode_xsens_rate_of_turn(data)
            elif can_id == 0x033:
                self.decode_xsens_delta_q(data)
            elif can_id == 0x034:
                self.decode_xsens_acceleration(data)
            elif can_id == 0x041:
                self.decode_xsens_magnetic_field(data)
            elif can_id == 0x071:
                self.decode_xsens_latlon(data)
            elif can_id == 0x072:
                self.decode_xsens_altitude(data)
            elif can_id == 0x076:
                self.decode_xsens_velocity(data)

        except Exception as e:
            print(f"Failed to decode CAN message ID 0x{can_id:03X}: {e}")

    # 所有decode函数（与原代码相同）
    def decode_timestamp(self, data):
        if len(data) >= 6:
            ms_since_midnight = struct.unpack('<I', data[0:4])[0]
            days_since_1984 = struct.unpack('<H', data[4:6])[0]
            
            base_timestamp = 441763200
            total_seconds = base_timestamp + (days_since_1984 * 86400) + (ms_since_midnight / 1000.0)
            decoded_time = datetime.fromtimestamp(total_seconds)
            
            current_time = time.time()
            self.data_store['timestamp']['time'] = decoded_time
            self.data_store['timestamp']['last_update'] = current_time

    def decode_vcu_cockpit(self, data):
        if len(data) >= 8:
            stear_raw = struct.unpack('<h', data[0:2])[0]
            accel_raw = data[2] 
            apps1_raw = data[3]
            apps2_raw = data[4]
            brake_raw = data[5]
            bse1_raw = data[6]
            bse2_raw = data[7]
            stear_data = stear_raw * 100
            
            current_time = time.time()
            self.data_store['vcu']['steer'] = stear_data
            self.data_store['vcu']['accel'] = accel_raw
            self.data_store['vcu']['apps1'] = apps1_raw
            self.data_store['vcu']['apps2'] = apps2_raw
            self.data_store['vcu']['brake'] = brake_raw
            self.data_store['vcu']['bse1'] = bse1_raw
            self.data_store['vcu']['bse2'] = bse2_raw
            self.data_store['vcu']['last_update'] = current_time

    def decode_vcu_suspension(self, data):
        if len(data) >= 4:
            suspF_raw = struct.unpack('<H', data[0:2])[0]
            suspF = suspF_raw * 0.0001 + 0.3
            
            suspR_raw = struct.unpack('<H', data[2:4])[0]
            suspR = suspR_raw * 0.0001 + 0.3
            
            current_time = time.time()
            self.data_store['vcu']['suspF'] = suspF
            self.data_store['vcu']['suspR'] = suspR
            self.data_store['vcu']['last_update'] = current_time

    def decode_gps_basic(self, data):
        if len(data) >= 8:
            lat_raw = struct.unpack('<i', data[0:4])[0]
            self.gps_lat = lat_raw / 10**7
            
            lon_raw = struct.unpack('<i', data[4:8])[0]
            self.gps_lon = lon_raw / 10**7
            
            current_time = time.time()
            self.data_store['gps']['lat'] = self.gps_lat
            self.data_store['gps']['lon'] = self.gps_lon
            self.data_store['gps']['last_update'] = current_time

    def decode_gps_extended(self, data):
        if len(data) >= 2:
            alt_raw = struct.unpack('<h', data[0:2])[0]
            status_byte = data[2] if len(data) > 2 else 0
            self.gps_alt = float(alt_raw)
            
            current_time = time.time()
            self.data_store['gps']['alt'] = self.gps_alt
            self.data_store['gps']['status'] = status_byte
            self.data_store['gps']['last_update'] = current_time

    def decode_position_covariance(self, data, index):
        if len(data) >= 8 and 0 <= index < 9:
            covariance_value = struct.unpack('<d', data[0:8])[0]
            self.position_covariance[index] = covariance_value

    def decode_position_covariance_type(self, data):
        if len(data) >= 1:
            self.position_covariance_type = struct.unpack('<B', data[0:1])[0]
            
            covariance_types = {
                0: "UNKNOWN",
                1: "APPROXIMATED", 
                2: "DIAGONAL_KNOWN",
                3: "KNOWN"
            }
            type_name = covariance_types.get(self.position_covariance_type, "UNKNOWN")
            
            current_time = time.time()
            self.data_store['covariance']['type'] = self.position_covariance_type
            self.data_store['covariance']['type_name'] = type_name
            self.data_store['covariance']['last_update'] = current_time

    def decode_velocity_x(self, data):
        if len(data) >= 4:
            vx_raw = struct.unpack('<i', data[0:4])[0]
            vx = vx_raw / 1000.0
            
            current_time = time.time()
            self.data_store['velocity']['linear_x'] = vx
            self.data_store['velocity']['last_update'] = current_time

    def decode_velocity_y(self, data):
        if len(data) >= 4:
            vy_raw = struct.unpack('<i', data[0:4])[0]
            vy = vy_raw / 1000.0
            
            current_time = time.time()
            self.data_store['velocity']['linear_y'] = vy
            self.data_store['velocity']['last_update'] = current_time

    def decode_velocity_z(self, data):
        if len(data) >= 4:
            vz_raw = struct.unpack('<i', data[0:4])[0]
            vz = vz_raw / 1000.0
            
            current_time = time.time()
            self.data_store['velocity']['linear_z'] = vz
            self.data_store['velocity']['last_update'] = current_time

    def decode_angular_x(self, data):
        if len(data) >= 4:
            wx_raw = struct.unpack('<i', data[0:4])[0]
            wx = wx_raw / 1000.0
            
            current_time = time.time()
            self.data_store['velocity']['angular_x'] = wx
            self.data_store['velocity']['last_update'] = current_time

    def decode_angular_y(self, data):
        if len(data) >= 4:
            wy_raw = struct.unpack('<i', data[0:4])[0]
            wy = wy_raw / 1000.0
            
            current_time = time.time()
            self.data_store['velocity']['angular_y'] = wy
            self.data_store['velocity']['last_update'] = current_time

    def decode_angular_z(self, data):
        if len(data) >= 4:
            wz_raw = struct.unpack('<i', data[0:4])[0]
            wz = wz_raw / 1000.0
            
            current_time = time.time()
            self.data_store['velocity']['angular_z'] = wz
            self.data_store['velocity']['last_update'] = current_time

    def decode_velocity_magnitude(self, data):
        if len(data) >= 4:
            vmag_raw = struct.unpack('<i', data[0:4])[0]
            vmag = vmag_raw / 1000.0
            speed_kmh = vmag * 3.6
            
            current_time = time.time()
            self.data_store['velocity']['magnitude'] = vmag
            self.data_store['velocity']['speed_kmh'] = speed_kmh
            self.data_store['velocity']['last_update'] = current_time

    def decode_distance(self, data):
        if len(data) >= 4:
            distance_mm = struct.unpack('<I', data[0:4])[0]
            distance_km = distance_mm / 1000000.0
            
            current_time = time.time()
            self.data_store['distance']['trip_distance_km'] = distance_km
            self.data_store['distance']['last_update'] = current_time

    def decode_cell_voltage(self, data):
        if len(data) >= 8:
            index = data[0]
            
            if index % 7 != 0 or index > 98:
                return
            
            voltages = []
            for i in range(1, min(8, len(data))):
                voltage = data[i] * 0.02
                voltages.append(voltage)
            
            current_time = time.time()
            for i, voltage in enumerate(voltages):
                array_index = index + i
                if array_index < 105:
                    self.data_store['accumulator']['cell_voltages'][array_index] = voltage
            
            self.data_store['accumulator']['last_update'] = current_time

    def decode_accumulator_temperature(self, data):
        if len(data) >= 8:
            index = data[0]
            
            if index % 7 != 0 or index > 217:
                return
            
            temperatures = []
            for i in range(1, min(8, len(data))):
                temp = data[i] - 32  
                temperatures.append(temp)
            
            current_time = time.time()
            for i, temp in enumerate(temperatures):
                array_index = index + i
                if array_index < 224: 
                    self.data_store['accumulator']['cell_temperatures'][array_index] = temp

            self.data_store['accumulator']['last_update'] = current_time

    def decode_accumulator_heartbeat(self, data):
        if len(data) >= 1:
            heartbeat = data[0] == 0x7F
            
            current_time = time.time()
            self.data_store['accumulator']['heartbeat'] = heartbeat
            self.data_store['accumulator']['last_update'] = current_time

    def decode_accumulator_status(self, data):
        if len(data) >= 7:
            status = data[0]
            temp_raw = struct.unpack('<h', data[1:3])[0]
            voltage_raw = struct.unpack('<I', data[3:7])[0] if len(data) >= 7 else 0
            
            temperature = temp_raw * 0.125
            voltage = voltage_raw / 1024.0
            
            current_time = time.time()
            self.data_store['accumulator']['status'] = status
            self.data_store['accumulator']['temperature'] = temperature
            self.data_store['accumulator']['voltage'] = voltage
            self.data_store['accumulator']['last_update'] = current_time

    def decode_accumulator_state(self, data):
        if len(data) >= 5:
            soc = data[0]
            current_raw = struct.unpack('<h', data[1:3])[0]
            capacity_raw = struct.unpack('<h', data[3:5])[0] if len(data) >= 5 else 0
            
            current = current_raw * 0.01
            capacity = capacity_raw * 0.01
            
            current_time = time.time()
            self.data_store['accumulator']['soc'] = soc
            self.data_store['accumulator']['current'] = current
            self.data_store['accumulator']['capacity'] = capacity
            self.data_store['accumulator']['last_update'] = current_time

    def decode_inverter_status(self, data, inv_num):
        if len(data) >= 6:
            status_word1 = data[0]
            status_word2 = data[1]
            feedback_torque_raw = struct.unpack('<h', data[2:4])[0]
            speed_raw = struct.unpack('<h', data[4:6])[0]
            feedback_torque = feedback_torque_raw / 1000.0 * 20
            speed = speed_raw
            if inv_num == (0x213-0x210):
                feedback_torque *= -1
            
            if inv_num in self.data_store['inverters']:
                current_time = time.time()
                self.data_store['inverters'][inv_num]['status'] = (status_word1, status_word2) 
                self.data_store['inverters'][inv_num]['torque'] = feedback_torque
                self.data_store['inverters'][inv_num]['speed'] = speed
                self.data_store['inverters'][inv_num]['last_update'] = current_time

    def decode_inverter_state(self, data, inv_num):
        if len(data) >= 4:
            dc_voltage_raw = struct.unpack('<H', data[0:2])[0]
            dc_current_raw = struct.unpack('<H', data[2:4])[0]
            
            dc_voltage = dc_voltage_raw / 100.0
            dc_current = dc_current_raw / 100.0
            
            if inv_num in self.data_store['inverters']:
                current_time = time.time()
                self.data_store['inverters'][inv_num]['dc_voltage'] = dc_voltage
                self.data_store['inverters'][inv_num]['dc_current'] = dc_current
                self.data_store['inverters'][inv_num]['last_update'] = current_time

    def decode_inverter_temperature(self, data, inv_num):
        if len(data) >= 6:
            inv_mos_temp_raw = struct.unpack('<h', data[0:2])[0]
            mcu_temp_raw = struct.unpack('<h', data[2:4])[0]
            motor_temp_raw = struct.unpack('<h', data[4:6])[0]
            
            inv_mos_temp = inv_mos_temp_raw * 0.1
            mcu_temp = mcu_temp_raw * 0.1
            motor_temp = motor_temp_raw * 0.1
            
            if inv_num in self.data_store['inverters']:
                current_time = time.time()
                self.data_store['inverters'][inv_num]['mos_temp'] = inv_mos_temp
                self.data_store['inverters'][inv_num]['mcu_temp'] = mcu_temp
                self.data_store['inverters'][inv_num]['motor_temp'] = motor_temp
                self.data_store['inverters'][inv_num]['last_update'] = current_time

    def decode_inverter_heartbeat(self, data, inv_num):
        if len(data) >= 1:
            heartbeat = data[0] == 0x05
            
            if inv_num in self.data_store['inverters']:
                current_time = time.time()
                self.data_store['inverters'][inv_num]['heartbeat'] = heartbeat
                self.data_store['inverters'][inv_num]['last_update'] = current_time

    def decode_inverter_control(self, data, inv_num):
        if len(data) >= 4:
            control_word = struct.unpack('<H', data[0:2])[0]
            target_torque_raw = struct.unpack('<h', data[2:4])[0]
            target_torque = target_torque_raw / 1000.0 * 20
            if inv_num == (0x213-0x210):
                target_torque *= -1
            if inv_num in self.data_store['inverters']:
                current_time = time.time()
                self.data_store['inverters'][inv_num]['control_word'] = control_word
                self.data_store['inverters'][inv_num]['target_torque'] = target_torque
                self.data_store['inverters'][inv_num]['last_update'] = current_time

    def decode_imu_accel_km6(self, data):
        if len(data) >= 6:
            x_raw = struct.unpack('<h', data[0:2])[0]
            y_raw = struct.unpack('<h', data[2:4])[0]
            z_raw = struct.unpack('<h', data[4:6])[0]
            
            x = x_raw * 0.001
            y = y_raw * 0.001
            z = z_raw * 0.001
            
            current_time = time.time()
            self.data_store['imu']['accel_km6']['x'] = x
            self.data_store['imu']['accel_km6']['y'] = y
            self.data_store['imu']['accel_km6']['z'] = z
            self.data_store['imu']['last_update'] = current_time

    def decode_imu_accel_km308(self, data):
        if len(data) >= 6:
            x_raw = struct.unpack('<h', data[0:2])[0]
            y_raw = struct.unpack('<h', data[2:4])[0]
            z_raw = struct.unpack('<h', data[4:6])[0]
            
            x = x_raw * 0.001
            y = y_raw * 0.001
            z = z_raw * 0.001
            
            current_time = time.time()
            self.data_store['imu']['accel_km308']['x'] = x
            self.data_store['imu']['accel_km308']['y'] = y
            self.data_store['imu']['accel_km308']['z'] = z
            self.data_store['imu']['last_update'] = current_time

    def decode_imu_gyro(self, data):
        if len(data) >= 6:
            x_raw = struct.unpack('<h', data[0:2])[0]
            y_raw = struct.unpack('<h', data[2:4])[0]
            z_raw = struct.unpack('<h', data[4:6])[0]
            
            x = x_raw * 0.1
            y = y_raw * 0.1
            z = z_raw * 0.1
            
            current_time = time.time()
            self.data_store['imu']['gyro']['x'] = x
            self.data_store['imu']['gyro']['y'] = y
            self.data_store['imu']['gyro']['z'] = z
            self.data_store['imu']['last_update'] = current_time

    def decode_imu_euler(self, data):
        if len(data) >= 6:
            roll_raw = struct.unpack('<h', data[0:2])[0]
            pitch_raw = struct.unpack('<h', data[2:4])[0]
            yaw_raw = struct.unpack('<h', data[4:6])[0]
            
            roll = roll_raw * 0.01
            pitch = pitch_raw * 0.01
            yaw = yaw_raw * 0.01
            
            current_time = time.time()
            self.data_store['imu']['euler']['roll'] = roll
            self.data_store['imu']['euler']['pitch'] = pitch
            self.data_store['imu']['euler']['yaw'] = yaw
            self.data_store['imu']['last_update'] = current_time

    def decode_imu_mag(self, data):
        if len(data) >= 6:
            x_raw = struct.unpack('<h', data[0:2])[0]
            y_raw = struct.unpack('<h', data[2:4])[0]
            z_raw = struct.unpack('<h', data[4:6])[0]
            
            x = x_raw * 0.1
            y = y_raw * 0.1
            z = z_raw * 0.1
            
            current_time = time.time()
            self.data_store['imu']['mag']['x'] = x
            self.data_store['imu']['mag']['y'] = y
            self.data_store['imu']['mag']['z'] = z
            self.data_store['imu']['last_update'] = current_time

    def decode_imu2_accel(self, data):
        if len(data) >= 6:
            x_raw = struct.unpack('<h', data[0:2])[0]
            y_raw = struct.unpack('<h', data[2:4])[0]
            z_raw = struct.unpack('<h', data[4:6])[0]
            
            x = x_raw * 0.001
            y = y_raw * 0.001
            z = z_raw * 0.001
            
            current_time = time.time()
            self.data_store['imu2']['accel']['x'] = x
            self.data_store['imu2']['accel']['y'] = y
            self.data_store['imu2']['accel']['z'] = z
            self.data_store['imu2']['last_update'] = current_time

    def decode_imu2_gyro(self, data):
        if len(data) >= 6:
            x_raw = struct.unpack('<h', data[0:2])[0]
            y_raw = struct.unpack('<h', data[2:4])[0]
            z_raw = struct.unpack('<h', data[4:6])[0]
            
            x = x_raw * 0.1
            y = y_raw * 0.1
            z = z_raw * 0.1
            
            current_time = time.time()
            self.data_store['imu2']['gyro']['x'] = x
            self.data_store['imu2']['gyro']['y'] = y
            self.data_store['imu2']['gyro']['z'] = z
            self.data_store['imu2']['last_update'] = current_time

    def decode_imu2_quaternion(self, data):
        if len(data) >= 8:
            w_raw = struct.unpack('<h', data[0:2])[0]
            x_raw = struct.unpack('<h', data[2:4])[0]
            y_raw = struct.unpack('<h', data[4:6])[0]
            z_raw = struct.unpack('<h', data[6:8])[0]
            
            w = w_raw * 0.0001
            x = x_raw * 0.0001
            y = y_raw * 0.0001
            z = z_raw * 0.0001
            
            current_time = time.time()
            self.data_store['imu2']['quaternion']['w'] = w
            self.data_store['imu2']['quaternion']['x'] = x
            self.data_store['imu2']['quaternion']['y'] = y
            self.data_store['imu2']['quaternion']['z'] = z
            self.data_store['imu2']['last_update'] = current_time

    def decode_xsens_quaternion(self, data):
        if len(data) >= 8:
            q0_raw = struct.unpack('>h', data[0:2])[0]
            q1_raw = struct.unpack('>h', data[2:4])[0]
            q2_raw = struct.unpack('>h', data[4:6])[0]
            q3_raw = struct.unpack('>h', data[6:8])[0]
            
            scale = 3.05176e-05
            q0 = q0_raw * scale
            q1 = q1_raw * scale
            q2 = q2_raw * scale
            q3 = q3_raw * scale
            
            current_time = time.time()
            self.data_store['xsens']['quaternion']['q0'] = q0
            self.data_store['xsens']['quaternion']['q1'] = q1
            self.data_store['xsens']['quaternion']['q2'] = q2
            self.data_store['xsens']['quaternion']['q3'] = q3
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_delta_v(self, data):
        if len(data) >= 7:
            x_raw = struct.unpack('>h', data[0:2])[0]
            y_raw = struct.unpack('>h', data[2:4])[0]
            z_raw = struct.unpack('>h', data[4:6])[0]
            exponent = data[6]
            
            x = x_raw * (-7.62939e-06)
            y = y_raw * (-7.62939e-06)
            z = z_raw * 7.62939e-06
            
            current_time = time.time()
            self.data_store['xsens']['delta_v']['x'] = x
            self.data_store['xsens']['delta_v']['y'] = y
            self.data_store['xsens']['delta_v']['z'] = z
            self.data_store['xsens']['delta_v']['exponent'] = exponent
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_rate_of_turn(self, data):
        if len(data) >= 6:
            gyr_x_raw = struct.unpack('>h', data[0:2])[0]
            gyr_y_raw = struct.unpack('>h', data[2:4])[0]
            gyr_z_raw = struct.unpack('>h', data[4:6])[0]
            
            gyr_x = gyr_x_raw * (-0.00195313)
            gyr_y = gyr_y_raw * (-0.00195313)
            gyr_z = gyr_z_raw * 0.00195313
            
            current_time = time.time()
            self.data_store['xsens']['rate_of_turn']['gyr_x'] = gyr_x
            self.data_store['xsens']['rate_of_turn']['gyr_y'] = gyr_y
            self.data_store['xsens']['rate_of_turn']['gyr_z'] = gyr_z
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_delta_q(self, data):
        if len(data) >= 8:
            dq0_raw = struct.unpack('>h', data[0:2])[0]
            dq1_raw = struct.unpack('>h', data[2:4])[0]
            dq2_raw = struct.unpack('>h', data[4:6])[0]
            dq3_raw = struct.unpack('>h', data[6:8])[0]
            
            scale = 3.05185e-05
            dq0 = dq0_raw * scale
            dq1 = dq1_raw * scale
            dq2 = dq2_raw * scale
            dq3 = dq3_raw * scale
            
            current_time = time.time()
            self.data_store['xsens']['delta_q']['dq0'] = dq0
            self.data_store['xsens']['delta_q']['dq1'] = dq1
            self.data_store['xsens']['delta_q']['dq2'] = dq2
            self.data_store['xsens']['delta_q']['dq3'] = dq3
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_acceleration(self, data):
        if len(data) >= 6:
            acc_x_raw = struct.unpack('>h', data[0:2])[0]
            acc_y_raw = struct.unpack('>h', data[2:4])[0]
            acc_z_raw = struct.unpack('>h', data[4:6])[0]
            
            acc_x = acc_x_raw * (-0.00390625)
            acc_y = acc_y_raw * (-0.00390625)
            acc_z = acc_z_raw * 0.00390625
            
            current_time = time.time()
            self.data_store['xsens']['acceleration']['acc_x'] = acc_x
            self.data_store['xsens']['acceleration']['acc_y'] = acc_y
            self.data_store['xsens']['acceleration']['acc_z'] = acc_z
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_magnetic_field(self, data):
        if len(data) >= 6:
            mag_x_raw = struct.unpack('>h', data[0:2])[0]
            mag_y_raw = struct.unpack('>h', data[2:4])[0]
            mag_z_raw = struct.unpack('>h', data[4:6])[0]
            
            mag_x = mag_x_raw * (-0.000976563)
            mag_y = mag_y_raw * (-0.000976563)
            mag_z = mag_z_raw * 0.000976563
            
            current_time = time.time()
            self.data_store['xsens']['magnetic_field']['mag_x'] = mag_x
            self.data_store['xsens']['magnetic_field']['mag_y'] = mag_y
            self.data_store['xsens']['magnetic_field']['mag_z'] = mag_z
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_latlon(self, data):
        if len(data) >= 8:
            lat_raw = struct.unpack('>i', data[0:4])[0]
            lon_raw = struct.unpack('>i', data[4:8])[0]
            
            lat = lat_raw * 5.96046e-08
            lon = lon_raw * 1.19209e-07
            
            current_time = time.time()
            self.data_store['xsens']['gps']['lat'] = lat
            self.data_store['xsens']['gps']['lon'] = lon
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_altitude(self, data):
        if len(data) >= 4:
            alt_raw = struct.unpack('>i', data[0:4])[0]
            alt = alt_raw * 3.05176e-05
            
            current_time = time.time()
            self.data_store['xsens']['gps']['alt'] = alt
            self.data_store['xsens']['last_update'] = current_time

    def decode_xsens_velocity(self, data):
        if len(data) >= 6:
            vel_x_raw = struct.unpack('>h', data[0:2])[0]
            vel_y_raw = struct.unpack('>h', data[2:4])[0]
            vel_z_raw = struct.unpack('>h', data[4:6])[0]
            
            vel_x = vel_x_raw * (-0.015625)
            vel_y = vel_y_raw * (-0.015625)
            vel_z = vel_z_raw * 0.015625
            
            current_time = time.time()
            self.data_store['xsens']['velocity']['vel_x'] = vel_x
            self.data_store['xsens']['velocity']['vel_y'] = vel_y
            self.data_store['xsens']['velocity']['vel_z'] = vel_z
            self.data_store['xsens']['last_update'] = current_time


# Global server instance
can_server = None

# FastAPI Routes
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("enhanced_racing_dashboard.html", {"request": request})

@app.get("/AMS", response_class=HTMLResponse)
async def ams_dashboard(request: Request):
    return templates.TemplateResponse("battery_dashboard _666.html", {"request": request})

@app.get("/TQ", response_class=HTMLResponse)
async def tq_dashboard(request: Request):
    return templates.TemplateResponse("dashchart.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def chart_dashboard(request: Request):
    return templates.TemplateResponse("chart_dashboard-v4.html", {"request": request})

@app.get("/IMU", response_class=HTMLResponse)
async def imu_dashboard(request: Request):
    return templates.TemplateResponse("imu_realtime_dashboard.html", {"request": request})

@app.get("/xsens", response_class=HTMLResponse)
async def xsens_dashboard(request: Request):
    return templates.TemplateResponse("xsens_dashboard.html", {"request": request})

@app.get('/api/data')
async def get_data():
    if can_server:
        return {
            'timestamp': can_server.data_store['timestamp']['time'].isoformat() if can_server.data_store['timestamp']['time'] else None,
            'gps': can_server.data_store['gps'],
            'velocity': can_server.data_store['velocity'],
            'distance': can_server.data_store['distance'],
            'accumulator': can_server.data_store['accumulator'],
            'inverters': can_server.data_store['inverters'],
            'vcu': can_server.data_store['vcu'],
            'imu': can_server.data_store['imu'],
            'imu2': can_server.data_store['imu2'],
            'xsens': can_server.data_store['xsens'],
            'message_count': can_server.message_count,
            'update_time': datetime.now().isoformat(),
            'vehicle_clients': len(vehicle_clients)
        }
    else:
        return {'error': 'CAN server not initialized'}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """处理网页客户端的WebSocket连接"""
    await websocket.accept()
    web_connections.append(websocket)
    print('Web client connected')
    
    # 连线时主动推送一次资料
    if can_server:
        await can_server.broadcast_data()
    
    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        print('Web client disconnected', e)
    finally:
        if websocket in web_connections:
            web_connections.remove(websocket)

@app.get('/api/status')
async def get_status():
    """获取服务器状态"""
    return {
        'vehicle_clients': len(vehicle_clients),
        'web_clients': len(web_connections),
        'message_count': can_server.message_count if can_server else 0,
        'clients_info': [
            {
                'id': client_id,
                'connected_at': info['connected_at'],
                'last_heartbeat': info['last_heartbeat'],
                'message_count': info['message_count']
            }
            for client_id, info in vehicle_clients.items()
        ]
    }

async def start_vehicle_data_server():
    """启动车辆数据接收服务器"""
    global can_server
    can_server = RemoteCANServer()
    
    # 启动broadcaster循环
    broadcaster_task = asyncio.create_task(can_server.broadcaster_loop())
    
    # 启动WebSocket服务器接收车辆数据
    async with websockets.serve(can_server.handle_vehicle_client, "0.0.0.0", DATA_PORT):
        print(f"Vehicle data server listening on port {DATA_PORT}")
        await broadcaster_task

@app.on_event("startup")
async def startup_event():
    """应用启动事件"""
    asyncio.create_task(start_vehicle_data_server())

if __name__ == '__main__':
    print("=" * 70)
    print("NTURT CAN Monitor Server - Remote Side")
    print("=" * 70)
    print(f"Web Dashboard: http://0.0.0.0:{WEB_PORT}")
    print(f"Vehicle Data Port: {DATA_PORT}")
    print("Press Ctrl+C to stop the server.")
    print("=" * 70)
    
    uvicorn.run(app, host='0.0.0.0', port=WEB_PORT)
