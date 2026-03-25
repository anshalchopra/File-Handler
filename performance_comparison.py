
import time
import asyncio
import threading
import multiprocessing
import math

# --- 1. THE WORK ---

def io_task(n):
    """Simulates waiting for an API (I/O Bound)."""
    time.sleep(1)
    return f"I/O Task {n} Done"

def cpu_task(n):
    """Simulates heavy math (CPU Bound)."""
    # Factorial of a large number to stress the CPU
    math.factorial(50000)
    return f"CPU Task {n} Done"

# --- 2. THE TEST METHODS ---

from concurrent.futures import ThreadPoolExecutor

def run_threads(task_func, items):
    print(f"\n🧵 Running ThreadPool (Safe Multithreading)...")
    start = time.perf_counter()
    # We limit the number of threads to 100 so it doesn't crash your Mac
    with ThreadPoolExecutor(max_workers=100) as executor:
        executor.map(task_func, items)
    end = time.perf_counter()
    print(f"⏱️ ThreadPool Time: {end - start:.2f}s")

async def run_asyncio(items):
    print(f"\n⚡ Running AsyncIO (Massive Concurrency)...")
    
    async def async_io_task(n):
        await asyncio.sleep(1) # Fair comparison: 1s sleep
        return n

    start = time.perf_counter()
    # AsyncIO can handle 100,000 tasks easily on 1 thread!
    await asyncio.gather(*(async_io_task(i) for i in items))
    end = time.perf_counter()
    print(f"⏱️ AsyncIO Time: {end - start:.2f}s")

# --- 3. EXECUTION ---

if __name__ == "__main__":
    # Let's use 10,000 tasks (Still a lot, but safe for this test)
    nums = range(500) 

    print("="*40)
    print(f"TEST: HANDLING {len(nums)} I/O TASKS")
    print("="*40)
    
    # This would crash if we used the old logic, but ThreadPool stays safe
    run_threads(io_task, nums)
    
    # This will be incredibly fast
    asyncio.run(run_asyncio(nums))
