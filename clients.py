# client.py
import requests
import time
import threading
import socket
import os
import sys

class Client:
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
        self.current_algorithm = None
        
        print(f"Starting Client: {self.client_id}")
        print(f"Supported algorithms: {list(self.algorithms.keys())}")
        
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
    
    def show_progress(self, current, total, algorithm):
        """Show progress bar for sorting"""
        if total == 0:
            return
        
        percentage = (current / total) * 100
        bar_length = 30
        filled_length = int(bar_length * current // total)
        bar = '=' * filled_length + '-' * (bar_length - filled_length)
        
        sys.stdout.write(f'\r{algorithm}: [{bar}] {percentage:.1f}% ({current}/{total})')
        sys.stdout.flush()
    
    def quick_sort(self, arr):
        """Quick sort implementation"""
        if len(arr) <= 1:
            return arr
        
        pivot = arr[len(arr) // 2]
        left = [x for x in arr if x < pivot]
        middle = [x for x in arr if x == pivot]
        right = [x for x in arr if x > pivot]
        
        return self.quick_sort(left) + middle + self.quick_sort(right)
    
    def quick_sort_with_progress(self, arr, depth=0, max_depth=0):
        """Quick sort with visual progress"""
        if len(arr) <= 1:
            return arr
        
        if depth == 0:
            max_depth = len(arr).bit_length()
            print(f"Quick Sort started on {len(arr)} elements")
            print(f"Estimated depth: {max_depth} levels")
        
        pivot = arr[len(arr) // 2]
        left = [x for x in arr if x < pivot]
        middle = [x for x in arr if x == pivot]
        right = [x for x in arr if x > pivot]
        
        # Show progress
        progress = (depth / max_depth) * 100 if max_depth > 0 else 0
        bar_length = 20
        filled = int(bar_length * progress / 100)
        bar = '=' * filled + '-' * (bar_length - filled)
        print(f'\rQuick Sort: [{bar}] {progress:.1f}% | Depth: {depth}', end='')
        
        sorted_left = self.quick_sort_with_progress(left, depth + 1, max_depth)
        sorted_right = self.quick_sort_with_progress(right, depth + 1, max_depth)
        
        if depth == 0:
            print(f"\nQuick Sort completed!")
        
        return sorted_left + middle + sorted_right
    
    def merge_sort(self, arr, level=0, max_level=0):
        """Merge sort with progress tracking"""
        if len(arr) <= 1:
            return arr
        
        if level == 0:
            max_level = len(arr).bit_length()
            print(f"Merge Sort started on {len(arr)} elements")
            print(f"Estimated levels: {max_level}")
        
        mid = len(arr) // 2
        left = self.merge_sort(arr[:mid], level + 1, max_level)
        right = self.merge_sort(arr[mid:], level + 1, max_level)
        
        # Show progress
        if max_level > 0:
            progress = (level / max_level) * 100
            bar_length = 20
            filled = int(bar_length * progress / 100)
            bar = '=' * filled + '-' * (bar_length - filled)
            print(f'\rMerge Sort: [{bar}] {progress:.1f}% | Level: {level}', end='')
        
        result = self.merge(left, right)
        
        if level == 0:
            print(f"\nMerge Sort completed!")
        
        return result
    
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
        """Bubble sort with detailed progress"""
        n = len(arr)
        print(f"Bubble Sort started on {n} elements")
        print(f"Total passes needed: ~{n}")
        
        for i in range(n):
            swapped = False
            
            for j in range(0, n - i - 1):
                if arr[j] > arr[j + 1]:
                    arr[j], arr[j + 1] = arr[j + 1], arr[j]
                    swapped = True
            
            # Show progress after each pass
            progress = ((i + 1) / n) * 100
            bar_length = 30
            filled = int(bar_length * progress / 100)
            bar = '=' * filled + '-' * (bar_length - filled)
            
            # Show some array samples to visualize sorting
            sample_start = arr[:3] if len(arr) >= 3 else arr
            sample_end = arr[-3:] if len(arr) >= 3 else []
            
            print(f'\rBubble Sort: [{bar}] {progress:.1f}% | Pass {i+1}/{n} | Sample: {sample_start}...{sample_end}', end='')
            
            if not swapped:
                break
        
        print(f"\nBubble Sort completed after {i+1} passes!")
        return arr
    
    def process_work(self):
        """Main work loop with enhanced progress visualization"""
        print("Starting work processor...")
        
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
                
                start_time = time.time()
                
                # Choose the appropriate sorting function
                if algorithm == 'bubblesort':
                    sorted_data = self.bubble_sort(data.copy())
                elif algorithm == 'quicksort':
                    sorted_data = self.quick_sort_with_progress(data.copy())
                elif algorithm == 'mergesort':
                    sorted_data = self.merge_sort(data.copy())
                else:
                    sorted_data = self.algorithms[algorithm](data.copy())
                
                processing_time = time.time() - start_time
                
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
                    'chunk_id': chunk_id
                }, timeout=10)
                
                if submit_response.status_code == 200:
                    print("Result submitted successfully!")
                else:
                    print("Submit failed")
                
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
    
    args = parser.parse_args()
    
    client = Client(args.server, args.name)
    
    try:
        client.process_work()
    except KeyboardInterrupt:
        print("Shutting down...")
        client.stop()

if __name__ == '__main__':
    main()