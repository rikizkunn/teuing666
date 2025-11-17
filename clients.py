# universal_client.py
import requests
import time
import threading
import socket
import os

class UniversalClient:
    def __init__(self, server_url, client_name=None):
        self.server_url = server_url
        self.client_id = client_name or f"{socket.gethostname()}_{os.getpid()}"
        self.algorithms = {
            'quicksort': self.quick_sort,
            'mergesort': self.merge_sort,
            'bubblesort': self.bubble_sort
        }
        self.running = True
        self.current_mode = None
        
        print(f"🚀 Starting Universal Client: {self.client_id}")
        print(f"📋 Supported algorithms: {list(self.algorithms.keys())}")
        
        self.register()
        self.start_heartbeat()
    
    def register(self):
        """Register with master server"""
        try:
            response = requests.post(f"{self.server_url}/api/register", json={
                'client_id': self.client_id,
                'capabilities': ['serial', 'parallel'],
                'algorithms': list(self.algorithms.keys()),
                'hostname': socket.gethostname()
            }, timeout=5)
            print(f"✅ Registered with master: {response.json()}")
        except Exception as e:
            print(f"❌ Registration failed: {e}")
    
    def start_heartbeat(self):
        """Send heartbeat every 3 seconds to stay connected"""
        def heartbeat_loop():
            while self.running:
                try:
                    response = requests.post(f"{self.server_url}/api/heartbeat", 
                                           json={'client_id': self.client_id},
                                           timeout=5)
                    if response.status_code == 200:
                        print(f"♥ Heartbeat sent - Mode: {self.current_mode or 'idle'}")
                    else:
                        print("⚠ Heartbeat failed, re-registering...")
                        self.register()
                except Exception as e:
                    print(f"💔 Heartbeat error: {e}")
                    self.register()
                
                time.sleep(3)
        
        threading.Thread(target=heartbeat_loop, daemon=True).start()
    
    def quick_sort(self, arr):
        """Quick sort implementation"""
        if len(arr) <= 1:
            return arr
        pivot = arr[len(arr) // 2]
        left = [x for x in arr if x < pivot]
        middle = [x for x in arr if x == pivot]
        right = [x for x in arr if x > pivot]
        return self.quick_sort(left) + middle + self.quick_sort(right)
    
    def merge_sort(self, arr):
        """Merge sort implementation"""
        if len(arr) <= 1:
            return arr
        
        mid = len(arr) // 2
        left = self.merge_sort(arr[:mid])
        right = self.merge_sort(arr[mid:])
        
        return self.merge(left, right)
    
    def merge(self, left, right):
        """Merge helper for merge sort"""
        result = []
        i = j = 0
        
        while i < len(left) and j < len(right):
            if left[i] < right[j]:
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1
        
        result.extend(left[i:])
        result.extend(right[j:])
        return result
    
    def bubble_sort(self, arr):
        """Bubble sort implementation"""
        n = len(arr)
        for i in range(n):
            for j in range(0, n - i - 1):
                if arr[j] > arr[j + 1]:
                    arr[j], arr[j + 1] = arr[j + 1], arr[j]
        return arr
    
    def process_work(self):
        """Main work loop - processes whatever work master assigns"""
        print("🔄 Starting work processor...")
        
        while self.running:
            try:
                # Get work from server
                response = requests.get(f"{self.server_url}/api/get-work/{self.client_id}", timeout=10)
                work = response.json()
                
                if work.get('status') == 'no_work':
                    if self.current_mode != 'idle':
                        print("💤 No work available, waiting...")
                        self.current_mode = 'idle'
                    time.sleep(5)
                    continue
                
                # Process the assigned work
                data = work['data']
                algorithm = work['algorithm']
                batch_id = work['batch_id']
                chunk_id = work.get('chunk_id', 0)
                mode = work.get('mode', 'unknown')
                
                if mode != self.current_mode:
                    self.current_mode = mode
                    print(f"🔄 Mode changed to: {mode.upper()}")
                
                print(f"⚡ Processing {mode} work - {len(data)} numbers using {algorithm}")
                
                start_time = time.time()
                sorted_data = self.algorithms[algorithm](data.copy())
                processing_time = time.time() - start_time
                
                # Submit result
                submit_response = requests.post(f"{self.server_url}/api/submit-work", json={
                    'batch_id': batch_id,
                    'client_id': self.client_id,
                    'processed_data': sorted_data,
                    'processing_time': processing_time,
                    'chunk_id': chunk_id
                }, timeout=10)
                
                if submit_response.status_code == 200:
                    print(f"✅ Completed in {processing_time:.3f}s")
                else:
                    print(f"❌ Submit failed")
                
            except requests.exceptions.Timeout:
                print("⏰ Request timeout")
            except requests.exceptions.ConnectionError:
                print("🔌 Connection error, retrying in 10s...")
                time.sleep(10)
            except Exception as e:
                print(f"💥 Error: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop the client"""
        self.running = False
        print("🛑 Client stopped")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Universal Sorting Client')
    parser.add_argument('--server', default='http://localhost:5000', help='Master server URL')
    parser.add_argument('--name', help='Custom client name')
    
    args = parser.parse_args()
    
    client = UniversalClient(args.server, args.name)
    
    try:
        client.process_work()
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        client.stop()

if __name__ == '__main__':
    main()