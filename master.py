# universal_master.py
from flask import Flask, render_template, jsonify, request
import random
import time
import json
from collections import defaultdict
from datetime import datetime
import threading

app = Flask(__name__)

# Storage
batches = {}
clients_connected = {}
sorting_progress = defaultdict(dict)
benchmark_results = []

def cleanup_clients():
    """Background thread to clean up disconnected clients"""
    while True:
        time.sleep(5)
        current_time = time.time()
        disconnected = []
        for client_id, client in clients_connected.items():
            if current_time - client['last_seen'] > 10:  # 10 seconds timeout
                disconnected.append(client_id)
        
        for client_id in disconnected:
            print(f"Client {client_id} disconnected")
            del clients_connected[client_id]

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_clients, daemon=True)
cleanup_thread.start()

@app.route('/')
def home():
    return render_template('dashboard.html')

# API Routes
@app.route('/api/generate', methods=['POST'])
def generate_numbers():
    """Generate random numbers with custom size"""
    data = request.json
    count = data.get('count', 10000)
    batch_id = f"batch_{int(time.time())}"
    
    numbers = [random.randint(1, 1000000) for _ in range(count)]
    batches[batch_id] = {
        'numbers': numbers,
        'count': count,
        'created_at': datetime.now().isoformat(),
        'algorithm': data.get('algorithm', 'quicksort')
    }
    
    return jsonify({
        'status': 'success',
        'batch_id': batch_id,
        'count': count,
        'sample_data': numbers[:50]
    })

@app.route('/api/register', methods=['POST'])
def register_client():
    """Register universal client"""
    data = request.json
    client_id = data['client_id']
    
    clients_connected[client_id] = {
        'id': client_id,
        'capabilities': data.get('capabilities', []),
        'algorithms': data.get('algorithms', ['quicksort']),
        'hostname': data.get('hostname', 'unknown'),
        'last_seen': time.time(),
        'status': 'idle',
        'registered_at': datetime.now().isoformat()
    }
    
    print(f"✅ Client registered: {client_id}")
    print(f"📊 Total clients: {len(clients_connected)}")
    
    return jsonify({'status': 'registered'})

@app.route('/api/heartbeat', methods=['POST'])
def heartbeat():
    """Update client heartbeat"""
    data = request.json
    client_id = data['client_id']
    
    if client_id in clients_connected:
        clients_connected[client_id]['last_seen'] = time.time()
        return jsonify({'status': 'updated'})
    else:
        # Re-register if not found
        return jsonify({'status': 'not_found'})

@app.route('/api/clients')
def get_clients():
    """Get connected clients"""
    return jsonify({
        'clients': clients_connected,
        'count': len(clients_connected)
    })

def get_idle_clients(algorithm):
    """Get all idle clients that support the algorithm"""
    return [
        client_id for client_id, client in clients_connected.items()
        if client['status'] == 'idle' and algorithm in client.get('algorithms', [])
    ]

@app.route('/api/start-serial', methods=['POST'])
def start_serial():
    """Start serial sorting - use ONE idle client"""
    data = request.json
    batch_id = data['batch_id']
    algorithm = data.get('algorithm', 'quicksort')
    
    if batch_id not in batches:
        return jsonify({'status': 'error', 'message': 'Batch not found'})
    
    # Get idle clients
    idle_clients = get_idle_clients(algorithm)
    
    if not idle_clients:
        return jsonify({'status': 'error', 'message': 'No idle clients available'})
    
    # Use the first idle client for serial processing
    assigned_client = idle_clients[0]
    
    # Reset progress
    sorting_progress[batch_id] = {
        'mode': 'serial',
        'algorithm': algorithm,
        'start_time': time.time(),
        'completed_chunks': 0,
        'total_chunks': 1,
        'chunks': {
            0: {  # Single chunk for serial
                'client_id': assigned_client,
                'status': 'assigned',
                'size': len(batches[batch_id]['numbers']),
                'processed_data': None,
                'processing_time': None
            }
        },
        'assigned_client': assigned_client
    }
    
    # Update client status
    clients_connected[assigned_client]['status'] = 'processing_serial'
    
    return jsonify({
        'status': 'started',
        'mode': 'serial',
        'assigned_client': assigned_client,
        'idle_clients_count': len(idle_clients),
        'total_numbers': len(batches[batch_id]['numbers'])
    })

@app.route('/api/start-parallel', methods=['POST'])
def start_parallel():
    """Start parallel sorting - use ALL idle clients"""
    data = request.json
    batch_id = data['batch_id']
    algorithm = data.get('algorithm', 'quicksort')
    
    if batch_id not in batches:
        return jsonify({'status': 'error', 'message': 'Batch not found'})
    
    # Get all idle clients
    idle_clients = get_idle_clients(algorithm)
    
    if not idle_clients:
        return jsonify({'status': 'error', 'message': 'No idle clients available'})
    
    # Split data among all idle clients
    numbers = batches[batch_id]['numbers']
    total_numbers = len(numbers)
    total_clients = len(idle_clients)
    chunk_size = total_numbers // total_clients
    
    # Reset progress
    sorting_progress[batch_id] = {
        'mode': 'parallel',
        'algorithm': algorithm,
        'start_time': time.time(),
        'completed_chunks': 0,
        'total_chunks': total_clients,
        'chunks': {},
        'assigned_clients': idle_clients
    }
    
    # Assign chunks to each idle client
    for i, client_id in enumerate(idle_clients):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < total_clients - 1 else total_numbers
        
        sorting_progress[batch_id]['chunks'][i] = {
            'client_id': client_id,
            'status': 'assigned',
            'start_idx': start_idx,
            'end_idx': end_idx,
            'size': end_idx - start_idx,
            'processed_data': None,
            'processing_time': None
        }
        
        clients_connected[client_id]['status'] = f'processing_chunk_{i}'
    
    return jsonify({
        'status': 'started',
        'mode': 'parallel',
        'total_clients': total_clients,
        'chunk_size': chunk_size,
        'total_numbers': total_numbers,
        'assigned_clients': idle_clients
    })

@app.route('/api/get-work/<client_id>')
def get_work(client_id):
    """Get assigned work for client"""
    # Update heartbeat
    if client_id in clients_connected:
        clients_connected[client_id]['last_seen'] = time.time()
    
    # Find work for this client
    for batch_id, progress in sorting_progress.items():
        for chunk_id, chunk_info in progress['chunks'].items():
            if (chunk_info['client_id'] == client_id and 
                chunk_info['status'] == 'assigned'):
                
                # Get the data for this chunk
                if progress['mode'] == 'serial':
                    data = batches[batch_id]['numbers']
                else:  # parallel
                    data = batches[batch_id]['numbers'][chunk_info['start_idx']:chunk_info['end_idx']]
                
                return jsonify({
                    'batch_id': batch_id,
                    'mode': progress['mode'],
                    'algorithm': progress['algorithm'],
                    'data': data,
                    'chunk_id': chunk_id
                })
    
    return jsonify({'status': 'no_work'})

@app.route('/api/submit-work', methods=['POST'])
def submit_work():
    """Receive processed work from client"""
    data = request.json
    batch_id = data['batch_id']
    client_id = data['client_id']
    processed_data = data['processed_data']
    processing_time = data['processing_time']
    chunk_id = data.get('chunk_id', 0)
    
    if batch_id not in sorting_progress:
        return jsonify({'status': 'error', 'message': 'Batch not found'})
    
    progress = sorting_progress[batch_id]
    
    if chunk_id in progress['chunks']:
        progress['chunks'][chunk_id]['status'] = 'completed'
        progress['chunks'][chunk_id]['processed_data'] = processed_data
        progress['chunks'][chunk_id]['processing_time'] = processing_time
        progress['completed_chunks'] += 1
        
        # Update client status
        clients_connected[client_id]['status'] = 'idle'
        
        # Check if all chunks are done
        if progress['completed_chunks'] >= progress['total_chunks']:
            # Combine results
            if progress['mode'] == 'serial':
                final_result = processed_data  # Serial already has full sorted data
            else:
                # For parallel, combine and sort chunks
                all_chunks = []
                for chunk_info in sorted(progress['chunks'].values(), key=lambda x: x.get('start_idx', 0)):
                    all_chunks.extend(chunk_info['processed_data'])
                final_result = all_chunks
            
            progress['final_result'] = final_result
            progress['total_time'] = time.time() - progress['start_time']
            
            # Save benchmark
            benchmark_results.append({
                'batch_id': batch_id,
                'mode': progress['mode'],
                'algorithm': progress['algorithm'],
                'total_numbers': len(final_result),
                'total_time': progress['total_time'],
                'clients_used': list(set(chunk['client_id'] for chunk in progress['chunks'].values())),
                'clients_count': len(progress['chunks']),
                'timestamp': datetime.now().isoformat()
            })
    
    return jsonify({'status': 'success'})

@app.route('/api/progress/<batch_id>')
def get_progress(batch_id):
    """Get sorting progress for a batch"""
    if batch_id not in sorting_progress:
        return jsonify({'status': 'not_found'})
    
    progress = sorting_progress[batch_id]
    
    response = {
        'batch_id': batch_id,
        'mode': progress['mode'],
        'algorithm': progress['algorithm'],
        'completed_chunks': progress['completed_chunks'],
        'total_chunks': progress['total_chunks'],
        'is_complete': progress['completed_chunks'] >= progress['total_chunks'],
        'chunks': progress['chunks']
    }
    
    if progress.get('final_result'):
        response['final_result'] = progress['final_result'][:100]
        response['total_time'] = progress.get('total_time', 0)
    
    return jsonify(response)

@app.route('/api/benchmarks')
def get_benchmarks():
    """Get all benchmark results"""
    return jsonify({'benchmarks': benchmark_results[-10:]})

@app.route('/api/batches')
def get_batches():
    """Get all batches"""
    batch_list = []
    for batch_id, batch_data in batches.items():
        batch_list.append({
            'id': batch_id,
            'count': batch_data['count'],
            'created_at': batch_data['created_at'],
            'algorithm': batch_data['algorithm'],
            'sample_data': batch_data['numbers'][:20]
        })
    
    return jsonify({'batches': batch_list})

if __name__ == '__main__':
    print("🚀 Starting Universal Sorting Master Server...")
    print("📡 Server will automatically use idle clients for work")
    print("💡 Run 'python universal_client.py' on multiple computers to scale!")
    app.run(host='0.0.0.0', port=5000, debug=True)