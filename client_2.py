# simple_computer2.py
import requests
import time
import threading

class SimpleClient:
    def __init__(self, server_url, client_id):
        self.server_url = server_url
        self.client_id = client_id
        self.register()
    
    def register(self):
        """Register with master server"""
        response = requests.get(f"{self.server_url}/register/{self.client_id}")
        print(f"Registered: {response.json()}")
    
    def quick_sort(self, arr):
        """Simple quick sort"""
        if len(arr) <= 1:
            return arr
        pivot = arr[len(arr) // 2]
        left = [x for x in arr if x < pivot]
        middle = [x for x in arr if x == pivot]
        right = [x for x in arr if x > pivot]
        return self.quick_sort(left) + middle + self.quick_sort(right)
    
    def process_serial(self):
        """Process all work serially"""
        print("Starting SERIAL processing...")
        
        while True:
            # Get work from server
            response = requests.get(f"{self.server_url}/get_work/{self.client_id}")
            work = response.json()
            
            if work.get('status') == 'no_work':
                print("No more work available")
                break
            
            # Process the chunk
            chunk = work['chunk']
            batch_id = work['batch_id']
            
            print(f"Processing chunk {work['chunk_index']} with {len(chunk)} numbers")
            
            start_time = time.time()
            sorted_chunk = self.quick_sort(chunk)
            process_time = time.time() - start_time
            
            # Submit result
            requests.post(f"{self.server_url}/submit", json={
                'batch_id': batch_id,
                'client_id': self.client_id,
                'sorted_chunk': sorted_chunk,
                'chunk_index': work['chunk_index'],
                'process_time': process_time
            })
            
            print(f"Chunk {work['chunk_index']} sorted in {process_time:.3f}s")
    
    def process_parallel(self):
        """Process work in parallel mode"""
        print("Starting PARALLEL processing...")
        self.process_serial()  # Same logic, but server distributes work
    
    def auto_mode(self):
        """Automatically switch between modes based on server"""
        while True:
            # Check available clients
            response = requests.get(f"{self.server_url}/clients")
            clients = response.json()['clients']
            
            if len(clients) > 1:
                print(f"Parallel mode available with {len(clients)} clients")
                self.process_parallel()
            else:
                print("Serial mode - only one client")
                self.process_serial()
            
            time.sleep(5)  # Wait before checking again

def main():
    server_url = "http://192.168.1.100:5000"  # Change to your server IP
    client = SimpleClient(server_url, "computer2")
    
    print("1. Serial Mode")
    print("2. Parallel Mode") 
    print("3. Auto Mode (detect)")
    
    choice = input("Choose mode: ")
    
    if choice == '1':
        client.process_serial()
    elif choice == '2':
        client.process_parallel()
    elif choice == '3':
        client.auto_mode()

if __name__ == '__main__':
    main()