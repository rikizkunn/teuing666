# algorithms.py
import time
import sys

def quick_sort(arr, show_progress=False):
    """Quick sort implementation with progress tracking"""
    if len(arr) <= 1:
        return arr
    
    if show_progress:
        print(f"Quick Sort: {len(arr)} elements")
    
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    
    return quick_sort(left, show_progress) + middle + quick_sort(right, show_progress)

def quick_sort_with_progress(arr):
    """Quick sort with visual progress"""
    if len(arr) <= 1:
        return arr
    
    max_depth = len(arr).bit_length()
    print(f"Quick Sort started on {len(arr)} elements")
    print(f"Estimated depth: {max_depth} levels")
    
    def _quick_sort(arr, depth=0):
        if len(arr) <= 1:
            return arr
        
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
        
        sorted_left = _quick_sort(left, depth + 1)
        sorted_right = _quick_sort(right, depth + 1)
        
        return sorted_left + middle + sorted_right
    
    result = _quick_sort(arr)
    print(f"\nQuick Sort completed!")
    return result

def merge_sort(arr, show_progress=False):
    """Merge sort implementation"""
    if len(arr) <= 1:
        return arr
    
    if show_progress:
        print(f"Merge Sort: {len(arr)} elements")
    
    mid = len(arr) // 2
    left = merge_sort(arr[:mid], show_progress)
    right = merge_sort(arr[mid:], show_progress)
    
    return _merge(left, right)

def merge_sort_with_progress(arr):
    """Merge sort with visual progress"""
    if len(arr) <= 1:
        return arr
    
    max_level = len(arr).bit_length()
    print(f"Merge Sort started on {len(arr)} elements")
    print(f"Estimated levels: {max_level}")
    
    def _merge_sort(arr, level=0):
        if len(arr) <= 1:
            return arr
        
        mid = len(arr) // 2
        left = _merge_sort(arr[:mid], level + 1)
        right = _merge_sort(arr[mid:], level + 1)
        
        # Show progress
        if max_level > 0:
            progress = (level / max_level) * 100
            bar_length = 20
            filled = int(bar_length * progress / 100)
            bar = '=' * filled + '-' * (bar_length - filled)
            print(f'\rMerge Sort: [{bar}] {progress:.1f}% | Level: {level}', end='')
        
        return _merge(left, right)
    
    result = _merge_sort(arr)
    print(f"\nMerge Sort completed!")
    return result

def _merge(left, right):
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

def bubble_sort(arr, show_progress=False):
    """Bubble sort implementation"""
    n = len(arr)
    if show_progress:
        print(f"Bubble Sort: {n} elements")
    
    for i in range(n):
        swapped = False
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
                swapped = True
        if not swapped:
            break
    return arr

def bubble_sort_with_progress(arr):
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

def heap_sort(arr, show_progress=False):
    """Heap sort implementation"""
    if show_progress:
        print(f"Heap Sort: {len(arr)} elements")
    
    def heapify(arr, n, i):
        largest = i
        left = 2 * i + 1
        right = 2 * i + 2

        if left < n and arr[i] < arr[left]:
            largest = left

        if right < n and arr[largest] < arr[right]:
            largest = right

        if largest != i:
            arr[i], arr[largest] = arr[largest], arr[i]
            heapify(arr, n, largest)

    n = len(arr)
    
    # Build max heap
    for i in range(n // 2 - 1, -1, -1):
        heapify(arr, n, i)
    
    # Extract elements
    for i in range(n - 1, 0, -1):
        arr[i], arr[0] = arr[0], arr[i]
        heapify(arr, i, 0)
    
    return arr

def heap_sort_with_progress(arr):
    """Heap sort with progress tracking"""
    n = len(arr)
    print(f"Heap Sort started on {n} elements")
    
    def heapify(arr, n, i, progress_callback=None):
        largest = i
        left = 2 * i + 1
        right = 2 * i + 2

        if left < n and arr[i] < arr[left]:
            largest = left

        if right < n and arr[largest] < arr[right]:
            largest = right

        if largest != i:
            arr[i], arr[largest] = arr[largest], arr[i]
            heapify(arr, n, largest, progress_callback)
    
    # Build max heap
    for i in range(n // 2 - 1, -1, -1):
        heapify(arr, n, i)
        progress = ((n // 2 - i) / (n // 2)) * 50
        bar_length = 25
        filled = int(bar_length * progress / 100)
        bar = '=' * filled + '-' * (bar_length - filled)
        print(f'\rHeap Sort: [{bar}] {progress:.1f}% | Building heap', end='')
    
    # Extract elements
    for i in range(n - 1, 0, -1):
        arr[i], arr[0] = arr[0], arr[i]
        heapify(arr, i, 0)
        progress = 50 + ((n - i) / n) * 50
        bar_length = 25
        filled = int(bar_length * progress / 100)
        bar = '=' * filled + '-' * (bar_length - filled)
        print(f'\rHeap Sort: [{bar}] {progress:.1f}% | Extracting elements', end='')
    
    print(f"\nHeap Sort completed!")
    return arr

def insertion_sort(arr, show_progress=False):
    """Insertion sort implementation"""
    if show_progress:
        print(f"Insertion Sort: {len(arr)} elements")
    
    for i in range(1, len(arr)):
        key = arr[i]
        j = i - 1
        while j >= 0 and key < arr[j]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key
    return arr

def insertion_sort_with_progress(arr):
    """Insertion sort with progress tracking"""
    n = len(arr)
    print(f"Insertion Sort started on {n} elements")
    
    for i in range(1, n):
        key = arr[i]
        j = i - 1
        while j >= 0 and key < arr[j]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key
        
        # Show progress
        progress = (i / n) * 100
        bar_length = 30
        filled = int(bar_length * progress / 100)
        bar = '=' * filled + '-' * (bar_length - filled)
        print(f'\rInsertion Sort: [{bar}] {progress:.1f}% | Element {i}/{n}', end='')
    
    print(f"\nInsertion Sort completed!")
    return arr

def selection_sort(arr, show_progress=False):
    """Selection sort implementation"""
    if show_progress:
        print(f"Selection Sort: {len(arr)} elements")
    
    for i in range(len(arr)):
        min_idx = i
        for j in range(i + 1, len(arr)):
            if arr[min_idx] > arr[j]:
                min_idx = j
        arr[i], arr[min_idx] = arr[min_idx], arr[i]
    return arr

def selection_sort_with_progress(arr):
    """Selection sort with progress tracking"""
    n = len(arr)
    print(f"Selection Sort started on {n} elements")
    
    for i in range(n):
        min_idx = i
        for j in range(i + 1, n):
            if arr[min_idx] > arr[j]:
                min_idx = j
        arr[i], arr[min_idx] = arr[min_idx], arr[i]
        
        # Show progress
        progress = (i / n) * 100
        bar_length = 30
        filled = int(bar_length * progress / 100)
        bar = '=' * filled + '-' * (bar_length - filled)
        print(f'\rSelection Sort: [{bar}] {progress:.1f}% | Pass {i+1}/{n}', end='')
    
    print(f"\nSelection Sort completed!")
    return arr

def tim_sort(arr, show_progress=False):
    """Python's built-in Timsort (for comparison)"""
    if show_progress:
        print(f"Tim Sort (Python built-in): {len(arr)} elements")
    return sorted(arr)

def tim_sort_with_progress(arr):
    """Tim sort with progress simulation"""
    print(f"Tim Sort (Python built-in) started on {len(arr)} elements")
    print("Note: Built-in sort doesn't show internal progress")
    
    # Simulate progress for consistency
    for i in range(101):
        progress = i
        bar_length = 30
        filled = int(bar_length * progress / 100)
        bar = '=' * filled + '-' * (bar_length - filled)
        print(f'\rTim Sort: [{bar}] {progress:.1f}%', end='')
        if i < 100:
            time.sleep(0.01)  # Brief pause to show progress
    
    print(f"\nTim Sort completed!")
    return sorted(arr)

# Algorithm configurations
ALGORITHMS = {
    'quicksort': (quick_sort_with_progress, "Quick Sort (O(n log n) average)"),
    'mergesort': (merge_sort_with_progress, "Merge Sort (O(n log n) guaranteed)"),
    'heapsort': (heap_sort_with_progress, "Heap Sort (O(n log n) guaranteed)"),
    'bubblesort': (bubble_sort_with_progress, "Bubble Sort (O(n²))"),
    'insertionsort': (insertion_sort_with_progress, "Insertion Sort (O(n²))"),
    'selectionsort': (selection_sort_with_progress, "Selection Sort (O(n²))"),
    'timsort': (tim_sort_with_progress, "Tim Sort (Python built-in)"),
}

# Simple versions without progress for internal use
SIMPLE_ALGORITHMS = {
    'quicksort': quick_sort,
    'mergesort': merge_sort,
    'heapsort': heap_sort,
    'bubblesort': bubble_sort,
    'insertionsort': insertion_sort,
    'selectionsort': selection_sort,
    'timsort': tim_sort,
}

def get_algorithm_info():
    """Get information about all available algorithms"""
    info = {}
    for name, (_, description) in ALGORITHMS.items():
        info[name] = description
    return info

def benchmark_algorithm(algorithm_name, data):
    """Benchmark a specific algorithm on given data"""
    if algorithm_name not in SIMPLE_ALGORITHMS:
        raise ValueError(f"Unknown algorithm: {algorithm_name}")
    
    algorithm = SIMPLE_ALGORITHMS[algorithm_name]
    
    # Create a copy to avoid modifying original
    test_data = data.copy()
    
    # Time the algorithm
    start_time = time.time()
    result = algorithm(test_data, show_progress=False)
    end_time = time.time()
    
    # Verify the result is sorted
    is_sorted = all(result[i] <= result[i+1] for i in range(len(result)-1))
    
    return {
        'algorithm': algorithm_name,
        'time': end_time - start_time,
        'is_sorted': is_sorted,
        'data_size': len(data)
    }