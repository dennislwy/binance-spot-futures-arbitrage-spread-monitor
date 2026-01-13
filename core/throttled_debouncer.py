import asyncio
from datetime import datetime
from typing import Callable, Optional


class ThrottledDebouncer:
    """Combines debouncing and throttling (max wait) for async function execution.
    
    This class provides a mechanism to control the execution frequency of async functions
    by combining two strategies:
    - Debouncing: Delays execution until calls stop arriving for a specified duration
    - Throttling: Ensures execution happens at least once within a maximum wait time
    
    Attributes:
        debounce_seconds (float): Debounce delay in seconds (0 = no debouncing).
        max_wait_seconds (float): Maximum wait time in seconds (0 = no throttling).
        callback (Optional[Callable]): The async function to be executed.
    
    Example:
        >>> async def my_func():
        ...     print("Executed!")
        >>> 
        >>> debouncer = ThrottledDebouncer(debounce_ms=100, max_wait_ms=1000, callback=my_func)
        >>> await debouncer.trigger()
    """
    
    def __init__(
        self,
        debounce_ms: int = 0,
        max_wait_ms: int = 0,
        callback: Optional[Callable] = None,
    ):
        """Initialize the ThrottledDebouncer.
        
        Args:
            debounce_ms (int): Delay execution until calls stop for this duration in milliseconds.
                0 means no debouncing (execute immediately). Defaults to 0.
            max_wait_ms (int): Maximum time to wait before forcing execution in milliseconds.
                0 means no throttling (wait indefinitely). Defaults to 0.
            callback (Optional[Callable]): The async function to execute. Can be set later
                via decorator pattern. Defaults to None.
        
        Raises:
            ValueError: If debounce_ms or max_wait_ms is negative.
            ValueError: If max_wait_ms is set and is less than debounce_ms.
        """
        # Validate input parameters
        if debounce_ms < 0 or max_wait_ms < 0:
            raise ValueError("debounce_ms and max_wait_ms must be non-negative")
        
        if max_wait_ms > 0 and debounce_ms > max_wait_ms:
            raise ValueError("max_wait_ms must be greater than or equal to debounce_ms")
        
        # Convert milliseconds to seconds for asyncio compatibility
        self.debounce_seconds = debounce_ms / 1000
        self.max_wait_seconds = max_wait_ms / 1000
        self.callback = callback
        
        # Internal state tracking
        self._debounce_task: Optional[asyncio.Task] = None  # Current pending debounce task
        self._last_call_time: Optional[float] = None  # Timestamp of most recent trigger call
        self._first_pending_call_time: Optional[float] = None  # Timestamp when current batch started
        
    async def trigger(self, *args, **kwargs):
        """Trigger the debounced/throttled execution of the callback.
        
        This method handles the core logic of debouncing and throttling. It will:
        - Execute immediately if both debounce and throttle are disabled
        - Wait for debounce period if calls stop arriving
        - Force execution after max_wait period even if calls continue.
        
        Args:
            *args: Positional arguments to pass to the callback function.
            **kwargs: Keyword arguments to pass to the callback function.
        
        Raises:
            ValueError: If no callback function has been set.
        
        Returns:
            None
        """
        if self.callback is None:
            raise ValueError("No callback set. Provide callback in __init__ or use as decorator.")
        
        # Special case: immediate execution when both debounce and throttle are disabled
        if self.debounce_seconds == 0 and self.max_wait_seconds == 0:
            await self.callback(*args, **kwargs)
            return
        
        # Record current time for debounce and throttle calculations
        current_time = asyncio.get_event_loop().time()
        self._last_call_time = current_time
        
        # Initialize the first call timestamp if this is the start of a new batch
        if self._first_pending_call_time is None:
            self._first_pending_call_time = current_time
        
        # Cancel any existing debounce task to reset the debounce timer
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        
        # Determine wait time based on throttling and debouncing rules
        if self.max_wait_seconds > 0:
            # Calculate how long we've been waiting since the first call in this batch
            time_since_first_call = current_time - self._first_pending_call_time
            
            # Force execution if max wait time has been exceeded
            if time_since_first_call >= self.max_wait_seconds:
                await self._execute(args, kwargs)
                return
            
            # Calculate remaining time until max wait is reached
            remaining_max_wait = self.max_wait_seconds - time_since_first_call
            
            # Choose the smaller of debounce time or remaining max wait time
            wait_time = min(self.debounce_seconds, remaining_max_wait) if self.debounce_seconds > 0 else remaining_max_wait
        else:
            # No throttling configured
            if self.debounce_seconds == 0:
                # No debounce either, execute immediately
                await self._execute(args, kwargs)
                return
            wait_time = self.debounce_seconds
        
        # Schedule execution after the calculated wait time
        self._debounce_task = asyncio.create_task(
            self._debounced_execute(wait_time, args, kwargs)
        )
    
    async def _debounced_execute(self, wait_time: float, args, kwargs):
        """Wait for debounce period then execute the callback.
        
        This internal method handles the actual waiting and execution. It can be
        cancelled if new trigger calls arrive before the wait completes.
        
        Args:
            wait_time (float): Time to wait in seconds before execution.
            args: Positional arguments to pass to the callback.
            kwargs: Keyword arguments to pass to the callback.
        
        Returns:
            None
        """
        try:
            # Wait for the debounce/throttle period
            await asyncio.sleep(wait_time)
            # If not cancelled, execute the callback
            await self._execute(args, kwargs)
        except asyncio.CancelledError:
            # Task was cancelled by a new trigger call, don't execute
            pass
    
    async def _execute(self, args, kwargs):
        """Execute the callback and reset internal state.
        
        This method performs the actual callback execution and resets the
        debouncer state to prepare for the next batch of calls.
        
        Args:
            args: Positional arguments to pass to the callback.
            kwargs: Keyword arguments to pass to the callback.
        
        Returns:
            None
        """
        # Reset state for the next batch of calls
        self._first_pending_call_time = None
        self._debounce_task = None
        
        # Execute the callback with provided arguments
        await self.callback(*args, **kwargs)
    
    def __call__(self, func: Callable):
        """Allow using the class as a decorator.
        
        This enables the decorator syntax:
        @ThrottledDebouncer(debounce_ms=100)
        async def my_function():
            pass
        
        Args:
            func (Callable): The async function to wrap with debouncing/throttling.
        
        Returns:
            Callable: A wrapper function that triggers the debounced execution.
        """
        # Set the decorated function as the callback
        self.callback = func
        
        async def wrapper(*args, **kwargs):
            """Wrapper that triggers the debounced execution."""
            await self.trigger(*args, **kwargs)
        
        return wrapper

if __name__ == "__main__":
    # Example usage scenarios demonstrating different configurations
    
    async def calc_spreads():
        """Simulated calculation function for demonstration purposes.
        
        This represents a typical async function that you might want to
        debounce/throttle, such as recalculating trading spreads.
        
        Returns:
            None
        """
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Executing calc_spreads()")
        await asyncio.sleep(0.05)  # Simulate some work


    async def demo_normal_debounce_with_throttle():
        """Demonstrate normal debouncing with throttling (max wait).
        
        Config: 100ms debounce + 1s max wait
        Behavior: Waits 100ms after calls stop, but forces execution after 1s
        even if calls continue.
        
        Returns:
            None
        """
        print("\n=== Normal: 100ms debounce + 1s throttle ===")
        
        throttled = ThrottledDebouncer(
            debounce_ms=100,
            max_wait_ms=1000,
            callback=calc_spreads
        )
        
        # Rapid continuous calls - should execute after 1 second due to throttle
        for i in range(25):
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Call {i}")
            await throttled.trigger()
            await asyncio.sleep(0.05)
        
        # Wait to see final debounced execution
        await asyncio.sleep(0.3)


    async def demo_no_throttle():
        """Demonstrate pure debouncing without throttling.
        
        Config: 100ms debounce, no max wait (max_wait_ms=0)
        Behavior: Only executes after calls stop for 100ms, no matter how long
        calls continue.
        
        Returns:
            None
        """
        print("\n=== No throttle: 100ms debounce only ===")
        
        debouncer = ThrottledDebouncer(
            debounce_ms=100,
            max_wait_ms=0,  # No throttling - can wait indefinitely
            callback=calc_spreads
        )
        
        # Continuous calls for 2 seconds - will only execute once at the end
        for i in range(40):
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Call {i}")
            await debouncer.trigger()
            await asyncio.sleep(0.05)
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Stopped calling, waiting for debounce...")
        await asyncio.sleep(0.3)


    async def demo_no_debounce():
        """Demonstrate throttling without debouncing.
        
        Config: No debounce (debounce_ms=0) + 1s max wait
        Behavior: Executes immediately on first call, then at most once per second.
        
        Returns:
            None
        """
        print("\n=== No debounce: throttle only (max every 1s) ===")
        
        throttler = ThrottledDebouncer(
            debounce_ms=0,  # No debouncing - execute immediately
            max_wait_ms=1000,  # But at most once per second
            callback=calc_spreads
        )
        
        # Should execute immediately on first call, then after 1 second
        for i in range(25):
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Call {i}")
            await throttler.trigger()
            await asyncio.sleep(0.05)
        
        await asyncio.sleep(0.3)


    async def demo_no_debounce_no_throttle():
        """Demonstrate immediate execution without debouncing or throttling.
        
        Config: No debounce, no throttle (both set to 0)
        Behavior: Executes immediately on every single call.
        
        Returns:
            None
        """
        print("\n=== No debounce, no throttle: immediate execution ===")
        
        immediate = ThrottledDebouncer(
            debounce_ms=0,
            max_wait_ms=0,
            callback=calc_spreads
        )
        
        # Should execute immediately on every call
        for i in range(5):
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Call {i}")
            await immediate.trigger()
            await asyncio.sleep(0.1)
        
        await asyncio.sleep(0.2)


    async def demo_decorator_style():
        """Demonstrate using ThrottledDebouncer as a decorator.
        
        Config: 100ms debounce + 500ms max wait, applied via @decorator syntax
        Behavior: Shows how to use the class as a function decorator.
        
        Returns:
            None
        """
        print("\n=== Decorator style ===")
        
        @ThrottledDebouncer(debounce_ms=100, max_wait_ms=500)
        async def process_data(data):
            """Example function decorated with ThrottledDebouncer."""
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Processing: {data}")
        
        # Multiple rapid calls - should be debounced/throttled
        for i in range(15):
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Call {i}")
            await process_data(f"data_{i}")
            await asyncio.sleep(0.05)
        
        await asyncio.sleep(0.3)

    async def main():
        """Run all demonstration scenarios."""
        await demo_normal_debounce_with_throttle()
        await demo_no_throttle()
        await demo_no_debounce()
        await demo_no_debounce_no_throttle()
        await demo_decorator_style()
        
    asyncio.run(main())