# simple_master.py
from flask import Flask, jsonify, request
import random
import time
import threading
from collections import defaultdict

app = Flask(__name__)

# Store data in memory (simple approach)
batches = {}
clients_connected = set()
sorting_progress = defaultdict(dict)

@app.route('/')
def home():
    return """
    <h1>Simple Parallel Sorting Server</h1>
    <button onclick="generateNumbers()">Generate 10,000 Numbers</button>
    <button onclick="startSerial()">Start Serial Sort</button>
    <button onclick="startParallel()">Start Parallel Sort</button>
    <div id="status"></div>
    <div id="progress"></div>
    <script>
        async function generateNumbers() {
            const response = await fetch('/generate/10000');
            const data = await response.json();
            document.getElementById('status').innerHTML = 'Generated: ' + data.batch_id;
        }
        
        async function startSerial() {
            const response = await fetch('/start/serial');
            const data = await response.json();
            document.getElementById('status').innerHTML = 'Started serial sort';
        }
        
        async function startParallel() {
            const response = await fetch('/start/parallel');
            const data = await response.json();
            document.getElementById('status').innerHTML = 'Started parallel sort';
        }
        
        // Check progress every second
        setInterval(async () => {
            const response = await fetch('/progress');
            const data = await response.json();
            document.getElementById('progress').innerHTML = 
                'Completed: ' + data.completed + '/' + data.total;
        }, 1000);
    </script>
    """

@app.route('/generate/<int:count>')
def generate_numbers(count):
    """Generate random numbers and store in memory"""
    batch_id = f"batch_{int(time.time())}"
    
    # Generate random numbers
    numbers = [random.randint(1, 1000000) for _ in range(count)]
    batches[batch_id] = numbers
    
    return jsonify({
        'status': 'success',
        'batch_id': batch_id,
        'count': count
    })

@app.route('/register/<client_id>')
def register_client(client_id):
    """Register a client computer"""
    clients_connected.add(client_id)
    return jsonify({'status': 'registered', 'client_id': client_id})

@app.route('/clients')
def list_clients():
    """List connected clients"""
    return jsonify({'clients': list(clients_connected)})

@app.route('/get_work/<client_id>')
def get_work(client_id):
    """Get work for a client"""
    # Find a batch that needs processing
    for batch_id, numbers in batches.items():
        if f'{batch_id}_completed' in sorting_progress:
            continue
            
        if f'{batch_id}_assigned_to_{client_id}' not in sorting_progress:
            # Calculate chunks based on number of connected clients
            total_clients = len(clients_connected)
            total_numbers = len(numbers)
            chunk_size = total_numbers // total_clients
            
            # Assign chunk to this client
            client_index = sorted(clients_connected).index(client_id)
            start_index = client_index * chunk_size
            end_index = start_index + chunk_size if client_index < total_clients - 1 else total_numbers
            
            chunk = numbers[start_index:end_index]
            
            # Mark as assigned
            sorting_progress[f'{batch_id}_assigned_to_{client_id}'] = True
            
            return jsonify({
                'batch_id': batch_id,
                'chunk': chunk,
                'chunk_index': client_index,
                'total_chunks': total_clients
            })
    
    return jsonify({'status': 'no_work'})

@app.route('/submit', methods=['POST'])
def submit_work():
    """Receive sorted work from client"""
    data = request.json
    batch_id = data['batch_id']
    client_id = data['client_id']
    sorted_chunk = data['sorted_chunk']
    chunk_index = data['chunk_index']
    
    # Store the sorted chunk
    if f'{batch_id}_results' not in sorting_progress:
        sorting_progress[f'{batch_id}_results'] = []
    
    sorting_progress[f'{batch_id}_results'].append({
        'chunk_index': chunk_index,
        'data': sorted_chunk,
        'client_id': client_id
    })
    
    # Check if all chunks are done
    total_clients = len(clients_connected)
    completed_chunks = len(sorting_progress[f'{batch_id}_results'])
    
    if completed_chunks >= total_clients:
        # Combine all sorted chunks
        all_chunks = sorting_progress[f'{batch_id}_results']
        all_chunks.sort(key=lambda x: x['chunk_index'])
        
        final_sorted = []
        for chunk in all_chunks:
            final_sorted.extend(chunk['data'])
        
        sorting_progress[f'{batch_id}_completed'] = final_sorted
        return jsonify({'status': 'batch_complete'})
    
    return jsonify({'status': 'chunk_received'})

@app.route('/start/<mode>')
def start_sort(mode):
    """Start sorting in specified mode"""
    # For demo, we'll just reset progress
    for key in list(sorting_progress.keys()):
        if key.endswith('_assigned_to_') or key.endswith('_results'):
            del sorting_progress[key]
    
    return jsonify({'status': f'started_{mode}', 'clients': len(clients_connected)})

@app.route('/progress')
def get_progress():
    """Get overall progress"""
    completed = 0
    total = len(clients_connected)
    
    for batch_id in batches:
        if f'{batch_id}_results' in sorting_progress:
            completed = len(sorting_progress[f'{batch_id}_results'])
    
    return jsonify({'completed': completed, 'total': total})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)