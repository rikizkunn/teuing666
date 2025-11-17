# enhanced_master.py
from flask import Flask, render_template, jsonify, request
import random
import time
import json
from collections import defaultdict
from datetime import datetime

app = Flask(__name__)

# Storage
batches = {}
clients_connected = {}
sorting_progress = defaultdict(dict)
benchmark_results = []

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
        'sample_data': numbers[:50]  # First 50 numbers for display
    })

@app.route('/api/register', methods=['POST'])
def register_client():
    """Register client with capabilities"""
    data = request.json
    client_id = data['client_id']
    
    clients_connected[client_id] = {
        'id': client_id,
        'capabilities': data.get('capabilities', []),
        'algorithms': data.get('algorithms', ['quicksort']),
        'last_seen': time.time(),
        'status': 'idle'
    }
    
    return jsonify({'status': 'registered'})

@app.route('/api/clients')
def get_clients():
    """Get connected clients"""
    # Clean up disconnected clients
    current_time = time.time()
    disconnected = []
    for client_id, client in clients_connected.items():
        if current_time - client['last_seen'] > 10:  # 10 seconds timeout
            disconnected.append(client_id)
    
    for client_id in disconnected:
        del clients_connected[client_id]
    
    return jsonify({'clients': clients_connected})

@app.route('/api/start-serial', methods=['POST'])
def start_serial():
    """Start serial sorting using only one computer"""
    data = request.json
    batch_id = data['batch_id']
    algorithm = data.get('algorithm', 'quicksort')
    
    if batch_id not in batches:
        return jsonify({'status': 'error', 'message': 'Batch not found'})
    
    # Reset progress
    sorting_progress[batch_id] = {
        'mode': 'serial',
        'algorithm': algorithm,
        'start_time': time.time(),
        'completed_chunks': 0,
        'total_chunks': 1,  # Serial processes everything as one chunk
        'chunks': {},
        'assigned_client': None
    }
    
    # Find a capable client
    capable_clients = [
        client_id for client_id, client in clients_connected.items()
        if algorithm in client.get('algorithms', [])
    ]
    
    if not capable_clients:
        return jsonify({'status': 'error', 'message': 'No capable clients available'})
    
    # Use the first capable client
    assigned_client = capable_clients[0]
    sorting_progress[batch_id]['assigned_client'] = assigned_client
    clients_connected[assigned_client]['status'] = 'processing_serial'
    
    return jsonify({
        'status': 'started',
        'assigned_client': assigned_client,
        'total_numbers': len(batches[batch_id]['numbers'])
    })

@app.route('/api/start-parallel', methods=['POST'])
def start_parallel():
    """Start parallel sorting using all computers"""
    data = request.json
    batch_id = data['batch_id']
    algorithm = data.get('algorithm', 'quicksort')
    
    if batch_id not in batches:
        return jsonify({'status': 'error', 'message': 'Batch not found'})
    
    # Get capable clients
    capable_clients = [
        client_id for client_id, client in clients_connected.items()
        if algorithm in client.get('algorithms', [])
    ]
    
    if not capable_clients:
        return jsonify({'status': 'error', 'message': 'No capable clients available'})
    
    # Calculate chunks - split data equally among clients
    numbers = batches[batch_id]['numbers']
    total_numbers = len(numbers)
    chunk_size = total_numbers // len(capable_clients)
    
    # Reset progress
    sorting_progress[batch_id] = {
        'mode': 'parallel',
        'algorithm': algorithm,
        'start_time': time.time(),
        'completed_chunks': 0,
        'total_chunks': len(capable_clients),
        'chunks': {},
        'assigned_clients': capable_clients
    }
    
    # Prepare chunks
    chunks = []
    for i, client_id in enumerate(capable_clients):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < len(capable_clients) - 1 else total_numbers
        chunk_data = numbers[start_idx:end_idx]
        
        sorting_progress[batch_id]['chunks'][i] = {
            'client_id': client_id,
            'status': 'assigned',
            'start_idx': start_idx,
            'end_idx': end_idx,
            'size': len(chunk_data),
            'processed_data': None,
            'processing_time': None
        }
        
        clients_connected[client_id]['status'] = f'processing_chunk_{i}'
    
    return jsonify({
        'status': 'started',
        'total_clients': len(capable_clients),
        'chunk_size': chunk_size,
        'total_numbers': total_numbers
    })

@app.route('/api/get-work/<client_id>')
def get_work(client_id):
    """Get assigned work for client"""
    for batch_id, progress in sorting_progress.items():
        if progress['mode'] == 'serial' and progress['assigned_client'] == client_id:
            if progress['completed_chunks'] == 0:  # Not started
                return jsonify({
                    'batch_id': batch_id,
                    'mode': 'serial',
                    'algorithm': progress['algorithm'],
                    'data': batches[batch_id]['numbers'],
                    'chunk_id': 0
                })
        
        elif progress['mode'] == 'parallel':
            for chunk_id, chunk_info in progress['chunks'].items():
                if chunk_info['client_id'] == client_id and chunk_info['status'] == 'assigned':
                    chunk_data = batches[batch_id]['numbers'][chunk_info['start_idx']:chunk_info['end_idx']]
                    return jsonify({
                        'batch_id': batch_id,
                        'mode': 'parallel',
                        'algorithm': progress['algorithm'],
                        'data': chunk_data,
                        'chunk_id': chunk_id,
                        'start_idx': chunk_info['start_idx'],
                        'end_idx': chunk_info['end_idx']
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
    
    if progress['mode'] == 'serial':
        progress['completed_chunks'] = 1
        progress['final_result'] = processed_data
        progress['total_time'] = time.time() - progress['start_time']
        progress['processing_time'] = processing_time
        
        # Save benchmark
        benchmark_results.append({
            'batch_id': batch_id,
            'mode': 'serial',
            'algorithm': progress['algorithm'],
            'total_numbers': len(processed_data),
            'total_time': progress['total_time'],
            'processing_time': processing_time,
            'client_used': client_id,
            'timestamp': datetime.now().isoformat()
        })
    
    elif progress['mode'] == 'parallel':
        if chunk_id in progress['chunks']:
            progress['chunks'][chunk_id]['status'] = 'completed'
            progress['chunks'][chunk_id]['processed_data'] = processed_data
            progress['chunks'][chunk_id]['processing_time'] = processing_time
            progress['completed_chunks'] += 1
            
            # Update client status
            clients_connected[client_id]['status'] = 'idle'
            
            # If all chunks completed, combine results
            if progress['completed_chunks'] >= progress['total_chunks']:
                # Combine all sorted chunks (they should be sorted individually)
                all_chunks = []
                for chunk_info in progress['chunks'].values():
                    all_chunks.extend(chunk_info['processed_data'])
                
                # Final sort to combine chunks (optional - chunks should already be sorted)
                all_chunks.sort()
                progress['final_result'] = all_chunks
                progress['total_time'] = time.time() - progress['start_time']
                
                # Save benchmark
                benchmark_results.append({
                    'batch_id': batch_id,
                    'mode': 'parallel',
                    'algorithm': progress['algorithm'],
                    'total_numbers': len(all_chunks),
                    'total_time': progress['total_time'],
                    'clients_used': list(set(chunk['client_id'] for chunk in progress['chunks'].values())),
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
        response['final_result'] = progress['final_result'][:100]  # First 100 for display
        response['total_time'] = progress.get('total_time', 0)
    
    return jsonify(response)

@app.route('/api/benchmarks')
def get_benchmarks():
    """Get all benchmark results"""
    return jsonify({'benchmarks': benchmark_results[-10:]})  # Last 10 results

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
    app.run(host='0.0.0.0', port=5000, debug=True)