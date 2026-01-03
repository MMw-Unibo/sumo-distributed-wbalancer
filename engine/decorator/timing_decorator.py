import time
import threading
import functools
from contextlib import contextmanager


# Thread-local storage for stats
thread_local_stats = threading.local()
def timed(block_name = "", mode = "set"):
    def decorator_timer(func):
        """Print the runtime of the decorated function"""
        # thread_local_stats = threading.local()
        @functools.wraps(func)
        def wrapper_timer(*args, **kwargs):
            if not hasattr(thread_local_stats, 'stats'):
                thread_local_stats.stats = {}

            start_time = time.time()
            value = func(*args, **kwargs)
            end_time = time.time()
            run_time = end_time - start_time
            
            if block_name not in thread_local_stats.stats:
                thread_local_stats.stats[block_name] = 0 if mode in ["set", "sum"] else []
            
            if mode == "set":
                thread_local_stats.stats[block_name] = run_time
            elif mode == "append":
                thread_local_stats.stats[block_name].append(run_time)
            elif mode == "sum":
                thread_local_stats.stats[block_name] += run_time
            else:
                raise ValueError("Invalid mode: {}".format(mode))
            return value
        return wrapper_timer
    return decorator_timer


@contextmanager
def timed_block(block_name, mode = "set"):
    # thread_local_stats = threading.local()
    if not hasattr(thread_local_stats, 'stats'):
        thread_local_stats.stats = {}
    
    start_time = time.time()
    yield
    end_time = time.time()
    run_time = end_time - start_time
    
    if block_name not in thread_local_stats.stats:
        thread_local_stats.stats[block_name] = 0 if mode in ["set", "sum"] else []

    if mode == "set":
        thread_local_stats.stats[block_name] = run_time
    elif mode == "append":
        thread_local_stats.stats[block_name].append(run_time)
    elif mode == "sum":
        thread_local_stats.stats[block_name] += run_time
    else:
        raise ValueError("Invalid mode: {}".format(mode))


def get_thread_stats():
    """Function to retrieve thread-local stats"""
    # thread_local_stats = threading.local()
    return getattr(thread_local_stats, 'stats', {})