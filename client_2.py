# enhanced_computer3.py
import requests
import time

class ParallelClient:
    def __init__(self, server_url, client_id):
        self.server_url = server_url
        self.client_id = client_id
        self.algorithms = {
            'quicksort': self.quick_sort,
            'mergesort': self.merge_sort
        }
        self.register()
    
    def register(self):
        """Register as parallel-only client"""
        response = requests.post(f"{self.server_url}/api/register", json={
            'client_id': self.client_id,
            'capabilities': ['parallel'],
            'algorithms': list(self.algorithms.keys())
        })
        print(f"Registered: {response.json()}")
    
    def quick_sort(self, arr):
        if len(arr) <= 1:
            return arr
        pivot = arr[len(arr) // 2]
        left = [x for x in arr if x < pivot]
        middle = [x for x in arr if x == pivot]
        right = [x for x in arr if x > pivot]
        return self.quick_sort(left) + middle + self.quick_sort(right)
    
    def merge_sort(self, arr):
        if len(arr) <= 1:
            return arr
        
        mid = len(arr) // 2
        left = self.merge_sort(arr[:mid])
        right = self.merge_sort(arr[mid:])
        
        return self.merge(left, right)
    
    def merge(self, left, right):
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
    
    def start_worker(self):
        """Continuous parallel worker"""
        print("Parallel worker started...")
        
        while True:
            try:
                # Get work from server
                response = requests.get(f"{self.server_url}/api/get-work/{self.client_id}")
                work = response.json()
                
                if work.get('status') == 'no_work':
                    time.sleep(2)
                    continue
                
                # Process the chunk
                data = work['data']
                algorithm = work['algorithm']
                batch_id = work['batch_id']
                chunk_id = work['chunk_id']
                
                print(f"Processing chunk {chunk_id} with {len(data)} numbers using {algorithm}")
                
                start_time = time.time()
                sorted_data = self.algorithms[algorithm](data.copy())
                processing_time = time.time() - start_time
                
                # Submit result
                requests.post(f"{self.server_url}/api/submit-work", json={
                    'batch_id': batch_id,
                    'client_id': self.client_id,
                    'processed_data': sorted_data,
                    'processing_time': processing_time,
                    'chunk_id': chunk_id
                })
                
                print(f"Chunk {chunk_id} completed in {processing_time:.3f}s")
                
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)

def main():
    server_url = "https://gdx1h4xsokf7.share.zrok.io"  # Change to your server IP
    client = ParallelClient(server_url, "computer3")
    client.start_worker()

if __name__ == '__main__':
    main()