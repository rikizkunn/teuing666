# simple_computer3.py
import requests
import time

class ParallelClient:
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
    
    def start_worker(self):
        """Continuous worker for parallel processing"""
        print("Parallel worker started...")
        
        while True:
            try:
                # Get work from server
                response = requests.get(f"{self.server_url}/get_work/{self.client_id}")
                work = response.json()
                
                if work.get('status') == 'no_work':
                    print("Waiting for work...")
                    time.sleep(2)
                    continue
                
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
                
                print(f"Chunk {work['chunk_index']} completed in {process_time:.3f}s")
                
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)

def main():
    server_url = "http://192.168.1.100:5000"  # Change to your server IP
    client = ParallelClient(server_url, "computer3")
    client.start_worker()

if __name__ == '__main__':
    main()