import threading
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

event_types = ["created", "deleted", "modified", "moved", "closed", "opened"]


class FSEventHandler(FileSystemEventHandler):
    """
    A custom event handler that triggers when a something changes in a monitored directory.

    Attributes:
        f (callable): A function that takes an event and returns a boolean.
        event_obj: event.
    """

    def __init__(self, f=lambda x: True):
        """
        f (callable): A function that takes an event and returns a boolean.
                      Defaults to always returning True (no filtering).
        """
        self.f = f
        self.event_obj = None
        self.event = threading.Event()

    def on_any_event(self, event):
        if self.f(event):
            self.event_obj = event
            self.event.set()


def wait_until(dir_list, f=lambda x: True, timeout=60):
    """
    Waits until a specified filesystem event occurs in any of the given directories.

    Args:
        dir_list (list): List of directories to monitor.
        f (callable): Optional filter function to apply to events.
        timeout (int): Maximum time (in seconds) to wait before giving up. Defaults to 30.

    Returns:
        tuple: (parent_directory, filename) if an event occurred, otherwise (None, None).
    """
    event_handler = FSEventHandler(f)
    observer = Observer()

    # Schedule the observer to monitor each directory in the list
    for directory in dir_list:
        directory = Path(directory).resolve()
        observer.schedule(event_handler, directory, recursive=False)

    observer.start()  # Start the observer thread

    # Wait for the event to be triggered or timeout
    event_handler.event.wait(timeout=timeout)

    # Cleanup: stop and join the observer thread
    observer.stop()
    observer.join()

    if event_handler.event_obj is not None:
        # If an event occurred, return the event_obj, parent directory and filename or dirname
        file = Path(event_handler.event_obj.src_path).resolve()
        return (
            event_handler.event_obj,
            event_handler.event_obj.event_type,
            file.parent,
            file.name,
        )

    return (None, None, None, None)  # Return None if no event occurred within timeout


def wait_until_file_event(dir_list, filenames, events=[], strict=False, timeout=60):
    def f(x):
        if x.event_type not in events:
            return False

        if x.is_directory:
            return False

        name = Path(x.src_path).resolve().name
        if len(filenames) == 0:
            return True
        
        for f in filenames:
            if strict:
                if f == name:
                    return True
            else:
                if f in name:
                    return True
        return False

    if events is None or len(events) == 0:
        events = event_types[:]
    return wait_until(dir_list, f, timeout=timeout)
