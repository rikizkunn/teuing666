# master.py
from flask import Flask, render_template, jsonify, request
import random
import time
import json
from collections import defaultdict
from datetime import datetime
import threading
import psutil
import platform

app = Flask(__name__)

# Storage
batches = {}
clients_connected = {}
sorting_progress = defaultdict(dict)
benchmark_results = []
performance_stats = {
    'fastest': None,
    'slowest': None,
    'latest_serial': None,
    'latest_parallel': None,
    'average_times': defaultdict(list)
}

def get_system_info():
    """Get master system information"""
    return {
        'cpu_cores': psutil.cpu_count(),
        'cpu_usage': psutil.cpu_percent(),
        'memory_total': psutil.virtual_memory().total,
        'memory_used': psutil.virtual_memory().used,
        'memory_percent': psutil.virtual_memory().percent,
        'platform': platform.system(),
        'platform_version': platform.version()
    }

def cleanup_clients():
    """Background thread to clean up disconnected clients"""
    while True:
        time.sleep(5)
        current_time = time.time()
        disconnected = []
        for client_id, client in clients_connected.items():
            if current_time - client['last_seen'] > 10:
                disconnected.append(client_id)
        
        for client_id in disconnected:
            print(f"Client {client_id} disconnected")
            del clients_connected[client_id]

def update_performance_stats(benchmark):
    """Update performance stats including latest serial/parallel"""
    # Update fastest
    if (performance_stats['fastest'] is None or 
        benchmark['total_time'] < performance_stats['fastest']['total_time']):
        performance_stats['fastest'] = benchmark
    
    # Update slowest  
    if (performance_stats['slowest'] is None or 
        benchmark['total_time'] > performance_stats['slowest']['total_time']):
        performance_stats['slowest'] = benchmark
    
    # Update latest serial/parallel
    if benchmark['mode'] == 'serial':
        performance_stats['latest_serial'] = benchmark
    elif benchmark['mode'] == 'parallel':
        performance_stats['latest_parallel'] = benchmark
    
    # Update average times by mode and algorithm
    key = f"{benchmark['mode']}_{benchmark['algorithm']}"
    performance_stats['average_times'][key].append(benchmark['total_time'])
    
    # Keep only last 10 for average
    if len(performance_stats['average_times'][key]) > 10:
        performance_stats['average_times'][key] = performance_stats['average_times'][key][-10:]

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_clients, daemon=True)
cleanup_thread.start()

@app.route('/')
def home():
    return render_template('dashboard.html')

@app.route('/batch/<batch_id>')
def batch_detail(batch_id):
    """Page showing full sorted and unsorted data"""
    if batch_id not in batches:
        return "Batch not found", 404
    
    batch_data = batches[batch_id]
    progress = sorting_progress.get(batch_id, {})
    
    return render_template('batch_detail.html', 
                         batch_id=batch_id,
                         batch_data=batch_data,
                         progress=progress)

@app.route('/api/system_info')
def get_system_info():
    """Get master system information"""
    return jsonify(get_system_info())

# API Routes
@app.route('/api/generate', methods=['POST'])
def generate_numbers():
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
    data = request.json
    client_id = data['client_id']
    
    clients_connected[client_id] = {
        'id': client_id,
        'capabilities': data.get('capabilities', []),
        'algorithms': data.get('algorithms', ['quicksort']),
        'hostname': data.get('hostname', 'unknown'),
        'system_info': data.get('system_info', {}),
        'last_seen': time.time(),
        'status': 'idle',
        'registered_at': datetime.now().isoformat()
    }
    
    print(f"Client registered: {client_id}")
    return jsonify({'status': 'registered'})

@app.route('/api/heartbeat', methods=['POST'])
def heartbeat():
    data = request.json
    client_id = data['client_id']
    
    if client_id in clients_connected:
        clients_connected[client_id]['last_seen'] = time.time()
        return jsonify({'status': 'updated'})
    else:
        return jsonify({'status': 'not_found'})

@app.route('/api/clients')
def get_clients():
    return jsonify({
        'clients': clients_connected,
        'count': len(clients_connected)
    })

def get_idle_clients(algorithm):
    return [
        client_id for client_id, client in clients_connected.items()
        if client['status'] == 'idle' and algorithm in client.get('algorithms', [])
    ]

@app.route('/api/start-serial', methods=['POST'])
def start_serial():
    data = request.json
    batch_id = data['batch_id']
    algorithm = data.get('algorithm', 'quicksort')
    
    if batch_id not in batches:
        return jsonify({'status': 'error', 'message': 'Batch not found'})
    
    idle_clients = get_idle_clients(algorithm)
    
    if not idle_clients:
        return jsonify({'status': 'error', 'message': 'No idle clients available'})
    
    assigned_client = idle_clients[0]
    
    sorting_progress[batch_id] = {
        'mode': 'serial',
        'algorithm': algorithm,
        'start_time': time.time(),
        'completed_chunks': 0,
        'total_chunks': 1,
        'chunks': {
            0: {
                'client_id': assigned_client,
                'chunk_id': 0,
                'status': 'assigned',
                'size': len(batches[batch_id]['numbers']),
                'processed_data': None,
                'processing_time': None
            }
        },
        'assigned_client': assigned_client
    }
    
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
    data = request.json
    batch_id = data['batch_id']
    algorithm = data.get('algorithm', 'quicksort')
    
    if batch_id not in batches:
        return jsonify({'status': 'error', 'message': 'Batch not found'})
    
    idle_clients = get_idle_clients(algorithm)
    
    if not idle_clients:
        return jsonify({'status': 'error', 'message': 'No idle clients available'})
    
    numbers = batches[batch_id]['numbers']
    total_numbers = len(numbers)
    total_clients = len(idle_clients)
    chunk_size = total_numbers // total_clients
    
    sorting_progress[batch_id] = {
        'mode': 'parallel',
        'algorithm': algorithm,
        'start_time': time.time(),
        'completed_chunks': 0,
        'total_chunks': total_clients,
        'chunks': {},
        'assigned_clients': idle_clients
    }
    
    for i, client_id in enumerate(idle_clients):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < total_clients - 1 else total_numbers
        
        sorting_progress[batch_id]['chunks'][i] = {
            'client_id': client_id,
            'chunk_id': i,
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
    if client_id in clients_connected:
        clients_connected[client_id]['last_seen'] = time.time()
    
    for batch_id, progress in sorting_progress.items():
        for chunk_id, chunk_info in progress['chunks'].items():
            if (chunk_info['client_id'] == client_id and 
                chunk_info['status'] == 'assigned'):
                
                if progress['mode'] == 'serial':
                    data = batches[batch_id]['numbers']
                else:
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
        
        clients_connected[client_id]['status'] = 'idle'
        
        if progress['completed_chunks'] >= progress['total_chunks']:
            if progress['mode'] == 'serial':
                final_result = processed_data
            else:
                all_chunks = []
                for chunk_info in sorted(progress['chunks'].values(), key=lambda x: x.get('start_idx', 0)):
                    all_chunks.extend(chunk_info['processed_data'])
                final_result = all_chunks
            
            progress['final_result'] = final_result
            progress['total_time'] = time.time() - progress['start_time']
            
            # Create benchmark record
            benchmark = {
                'batch_id': batch_id,
                'mode': progress['mode'],
                'algorithm': progress['algorithm'],
                'total_numbers': len(final_result),
                'total_time': progress['total_time'],
                'clients_used': list(set(chunk['client_id'] for chunk in progress['chunks'].values())),
                'clients_count': len(progress['chunks']),
                'timestamp': datetime.now().isoformat()
            }
            
            benchmark_results.append(benchmark)
            update_performance_stats(benchmark)
            print(f"Benchmark saved: {benchmark['mode']} {benchmark['algorithm']} - {benchmark['total_time']:.3f}s")
    
    return jsonify({'status': 'success'})

@app.route('/api/progress/<batch_id>')
def get_progress(batch_id):
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
    """Get all benchmark results with performance stats"""
    return jsonify({
        'benchmarks': benchmark_results[-10:],
        'performance_stats': performance_stats
    })

@app.route('/api/batch/<batch_id>')
def get_batch_data(batch_id):
    """Get full batch data for detail page"""
    if batch_id not in batches:
        return jsonify({'status': 'not_found'})
    
    batch_data = batches[batch_id]
    progress = sorting_progress.get(batch_id, {})
    
    response = {
        'batch_id': batch_id,
        'numbers': batch_data['numbers'],
        'count': batch_data['count'],
        'algorithm': batch_data['algorithm'],
        'created_at': batch_data['created_at']
    }
    
    if progress.get('final_result'):
        response['sorted_numbers'] = progress['final_result']
        response['total_time'] = progress.get('total_time', 0)
        response['is_complete'] = True
    else:
        response['is_complete'] = False
    
    return jsonify(response)

@app.route('/api/batches')
def get_batches():
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
    print("Starting Master Server...")
    app.run(host='0.0.0.0', port=5000, debug=True)