# Timeout checking utilities
import time
from collections import deque   

ALGO_TIME_LIMIT = 9*60 + 30
BEGIN_TIME = 0



def set_begin_time():
    """Set the start time for algorithm execution"""
    global BEGIN_TIME
    BEGIN_TIME = time.time()

def is_timeout() -> bool:
    """Check if algorithm has exceeded time limit"""
    return time.time() - BEGIN_TIME > ALGO_TIME_LIMIT

def get_remaining_time() -> float:
    """Get remaining time in seconds"""
    return max(0, ALGO_TIME_LIMIT - (time.time() - BEGIN_TIME))
