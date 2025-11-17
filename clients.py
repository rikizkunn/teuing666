# client.py
import requests
import time
import threading
import socket
import os
import sys
import platform
from algorithms import ALGORITHMS, SIMPLE_ALGORITHMS, get_algorithm_info

class Client:
    def __init__(self, server_url, client_name=None):
        self.server_url = server_url
        self.client_id = client_name or f"{socket.gethostname()}_{os.getpid()}"
        self.algorithms = list(ALGORITHMS.keys())
        self.running = True
        self.current_mode = None
        self.current_algorithm = None
        
        print(f"Starting Client: {self.client_id}")
        print(f"Supported algorithms: {', '.join(self.algorithms)}")
        
        self.register()
        self.start_heartbeat()
    
    def get_system_info(self):
        """Get client system information (Windows compatible)"""
        try:
            system_info = {
                'platform': platform.system(),
                'platform_version': platform.version(),
                'hostname': socket.gethostname(),
                'processor': platform.processor(),
                'architecture': platform.architecture()[0],
                'python_version': platform.python_version()
            }
            
            # Try to get Windows-specific info
            if platform.system() == "Windows":
                try:
                    import psutil
                    system_info.update({
                        'cpu_cores': psutil.cpu_count(),
                        'cpu_usage': psutil.cpu_percent(),
                        'memory_total': psutil.virtual_memory().total,
                        'memory_used': psutil.virtual_memory().used,
                        'memory_percent': psutil.virtual_memory().percent
                    })
                except ImportError:
                    print("psutil not available, skipping detailed system info")
            
            return system_info
        except Exception as e:
            print(f"Error getting system info: {e}")
            return {
                'platform': platform.system(),
                'hostname': socket.gethostname()
            }
    
    def register(self):
        """Register with master server"""
        try:
            response = requests.post(f"{self.server_url}/api/register", json={
                'client_id': self.client_id,
                'capabilities': ['serial', 'parallel'],
                'algorithms': self.algorithms,
                'algorithm_info': get_algorithm_info(),
                'hostname': socket.gethostname(),
                'system_info': self.get_system_info()
            }, timeout=5)
            print(f"Registered with master: {response.json()}")
        except Exception as e:
            print(f"Registration failed: {e}")
    
    def start_heartbeat(self):
        """Send heartbeat every 3 seconds"""
        def heartbeat_loop():
            while self.running:
                try:
                    response = requests.post(f"{self.server_url}/api/heartbeat", 
                                           json={'client_id': self.client_id},
                                           timeout=5)
                    if response.status_code == 200:
                        status = f"Mode: {self.current_mode or 'idle'}"
                        if self.current_algorithm:
                            status += f" | Algorithm: {self.current_algorithm}"
                        print(f"Heartbeat - {status}")
                except Exception as e:
                    print(f"Heartbeat error: {e}")
                    self.register()
                
                time.sleep(3)
        
        threading.Thread(target=heartbeat_loop, daemon=True).start()
    
    def measure_network_speed(self, data_size=1000):
        """Measure network speed by sending test data"""
        try:
            test_data = list(range(data_size))
            
            # Measure upload speed
            start_time = time.time()
            response = requests.post(f"{self.server_url}/api/network-test", 
                                   json={'data': test_data, 'client_id': self.client_id},
                                   timeout=10)
            upload_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                download_time = result.get('processing_time', 0)
                
                upload_speed = (data_size * 4) / upload_time  # Approximate bytes per second
                download_speed = (data_size * 4) / download_time if download_time > 0 else 0
                
                print(f"Network Speed Test:")
                print(f"  Upload: {upload_speed:.2f} bytes/sec")
                print(f"  Download: {download_speed:.2f} bytes/sec")
                print(f"  Round-trip: {upload_time + download_time:.3f}s")
                
                return {
                    'upload_speed': upload_speed,
                    'download_speed': download_speed,
                    'round_trip_time': upload_time + download_time
                }
        except Exception as e:
            print(f"Network speed test failed: {e}")
        
        return None
    
    def process_work(self):
        """Main work loop with enhanced progress visualization"""
        print("Starting work processor...")
        
        # Run initial network speed test
        print("Performing initial network speed test...")
        network_info = self.measure_network_speed(1000)
        
        while self.running:
            try:
                response = requests.get(f"{self.server_url}/api/get-work/{self.client_id}", timeout=10)
                work = response.json()
                
                if work.get('status') == 'no_work':
                    if self.current_mode != 'idle':
                        print("No work available, waiting...")
                        self.current_mode = 'idle'
                        self.current_algorithm = None
                    time.sleep(5)
                    continue
                
                # Process the assigned work
                data = work['data']
                algorithm = work['algorithm']
                batch_id = work['batch_id']
                chunk_id = work.get('chunk_id', 0)
                mode = work.get('mode', 'unknown')
                
                if mode != self.current_mode or algorithm != self.current_algorithm:
                    self.current_mode = mode
                    self.current_algorithm = algorithm
                    print(f"Mode changed to: {mode.upper()} | Algorithm: {algorithm}")
                
                print(f"Starting {mode} work")
                print(f"Chunk {chunk_id} | {len(data)} numbers | Algorithm: {algorithm}")
                print(f"Data range: {min(data)} to {max(data)}")
                
                # Record start time for network measurement
                work_start_time = time.time()
                
                # Choose the appropriate sorting function
                if algorithm in ALGORITHMS:
                    sort_function, _ = ALGORITHMS[algorithm]
                    sorted_data = sort_function(data.copy())
                else:
                    # Fallback to simple version
                    sorted_data = SIMPLE_ALGORITHMS.get(algorithm, SIMPLE_ALGORITHMS['quicksort'])(data.copy(), show_progress=False)
                    print(f"Using simple {algorithm} (no progress display)")
                
                processing_time = time.time() - work_start_time
                
                # Verify sort
                is_sorted = all(sorted_data[i] <= sorted_data[i+1] for i in range(len(sorted_data)-1))
                status = "SUCCESS" if is_sorted else "FAILED"
                
                print(f"Chunk {chunk_id} completed in {processing_time:.3f}s")
                print(f"Result: {len(sorted_data)} numbers | Sorted: {is_sorted}")
                print(f"First 5: {sorted_data[:5]}... Last 5: {sorted_data[-5:]}")
                
                # Submit result
                submit_response = requests.post(f"{self.server_url}/api/submit-work", json={
                    'batch_id': batch_id,
                    'client_id': self.client_id,
                    'processed_data': sorted_data,
                    'processing_time': processing_time,
                    'chunk_id': chunk_id,
                    'network_time': time.time() - work_start_time  # Total time including network
                }, timeout=10)
                
                if submit_response.status_code == 200:
                    print("Result submitted successfully!")
                else:
                    print("Submit failed")
                
                # Run periodic network speed test (every 10 minutes)
                if int(time.time()) % 600 == 0:  # Every 10 minutes
                    print("Running periodic network speed test...")
                    self.measure_network_speed(1000)
                
            except requests.exceptions.Timeout:
                print("Request timeout")
            except requests.exceptions.ConnectionError:
                print("Connection error, retrying in 10s...")
                time.sleep(10)
            except KeyboardInterrupt:
                print("Client stopping...")
                self.stop()
                break
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop the client"""
        self.running = False
        print("Client stopped")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Sorting Client')
    parser.add_argument('--server', default='http://localhost:5000', help='Master server URL')
    parser.add_argument('--name', help='Custom client name')
    parser.add_argument('--network-test', action='store_true', help='Run network test and exit')
    
    args = parser.parse_args()
    
    client = Client(args.server, args.name)
    
    if args.network_test:
        print("Running network speed test...")
        client.measure_network_speed(5000)
        return
    
    try:
        client.process_work()
    except KeyboardInterrupt:
        print("Shutting down...")
        client.stop()

if __name__ == '__main__':
    main()