"""
Utility Module: Custom Observer-Based Logging System.

This module implements the Gang of Four (GoF) 'Observer' design pattern 
to manage logging across the data pipeline. It decouples the act of logging 
(the Subject) from the destination of the logs (the Observers), allowing 
for highly extensible and scalable monitoring.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List

class LogObserver(ABC):
    """
    The Abstract Blueprint for all Log Observers.

    This interface establishes the contract that any concrete observer 
    must fulfill. It ensures that the Subject (Publisher) can safely 
    push updates to any subscribed observer without knowing its specific 
    implementation details.
    """

    @abstractmethod
    def update(self, level: str, message: str) -> None:
        """
        Receives the broadcasted log event from the Subject.

        Args:
            level (str): The severity level of the log (e.g., 'INFO', 'ERROR').
            message (str): The actual log message content.
        """
        pass

class ConsoleObserver(LogObserver):
    """
    Concrete Observer: Terminal Output.

    This observer listens for log events and prints them to the standard 
    console output. It applies basic ANSI color coding to help developers 
    visually parse the severity of messages during live execution.
    """

    def update(self, level: str, message: str) -> None:
        """
        Formats and prints the log message to the console with color codes.

        Args:
            level (str): The severity level determining the color.
            message (str): The log message content.
        """
        # ANSI color codes for terminal formatting
        colors = {
            "INFO": "\033[94m",    # Blue
            "WARNING": "\033[93m", # Yellow
            "ERROR": "\033[91m",   # Red
            "ENDC": "\033[0m"      # Reset
        }
        color = colors.get(level, colors["ENDC"])
        print(f"{color}[{level}] {message}{colors['ENDC']}")

class FileObserver(LogObserver):
    """
    Concrete Observer: Persistent File Storage.

    This observer listens for log events and appends them to a local text file. 
    It ensures that a permanent audit trail is kept for pipeline runs, 
    adding timestamps to every entry for historical debugging.

    Attributes:
        filepath (Path): The absolute path to the target log file.
    """

    def __init__(self, filepath: Path) -> None:
        """
        Initializes the FileObserver and ensures the target directory exists.

        Args:
            filepath (Path): The path where the log file will be stored.
        """
        self.filepath = filepath
        # Ensure the parent logs directory exists before attempting to write
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        
    def update(self, level: str, message: str) -> None:
        """
        Appends the timestamped log message to the persistent file.

        Args:
            level (str): The severity level.
            message (str): The log message content.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.filepath, "a", encoding="utf-8") as file:
            file.write(f"{timestamp} - [{level}] - {message}\n")

class LogSubject(ABC):
    """
    The Abstract Blueprint for the Log Publisher.

    This class manages the subscription list of observers. It provides 
    methods to attach or detach observers dynamically at runtime, and 
    handles the iteration required to notify all subscribers when an event occurs.

    Attributes:
        _observers (List[LogObserver]): The internal list of subscribed observers.
    """

    def __init__(self) -> None:
        """Initializes an empty list of observers."""
        self._observers: List[LogObserver] = []

    def attach(self, observer: LogObserver) -> None:
        """
        Subscribes a new observer to the notification list.

        Args:
            observer (LogObserver): The observer instance to attach.
        """
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: LogObserver) -> None:
        """
        Unsubscribes an observer from the notification list.

        Args:
            observer (LogObserver): The observer instance to remove.
        """
        if observer in self._observers:
            self._observers.remove(observer)

    def notify(self, level: str, message: str) -> None:
        """
        Broadcasts the log event to all currently attached observers.

        Args:
            level (str): The severity level of the event.
            message (str): The core message to broadcast.
        """
        for observer in self._observers:
            observer.update(level, message)

class PipelineLogger(LogSubject):
    """
    The Concrete Log Publisher (The Central Logger).

    This class provides the actual methods (`info`, `warning`, `error`) 
    that the pipeline code will interact with. When one of these methods 
    is called, it triggers the `notify` process inherited from `LogSubject`.
    """
    
    def info(self, message: str) -> None:
        """Logs an informational message (standard execution flow)."""
        self.notify("INFO", message)

    def warning(self, message: str) -> None:
        """Logs a warning message (non-critical issues)."""
        self.notify("WARNING", message)

    def error(self, message: str) -> None:
        """Logs an error message (critical failures or API rejections)."""
        self.notify("ERROR", message)

def get_logger(filename: str = "pipeline.log") -> PipelineLogger:
    """
    Factory Function: Assembles and configures the standard logging system.

    This helper function creates a PipelineLogger instance, locates the 
    project root to define the log directory, and automatically attaches 
    the ConsoleObserver and FileObserver.

    Args:
        filename (str, optional): The name of the output file. Defaults to "pipeline.log".

    Returns:
        PipelineLogger: The fully configured Subject ready to accept messages.
    """
    logger = PipelineLogger()
    
    # Define where the log file should live dynamically relative to this script
    project_root = Path(__file__).resolve().parent.parent.parent
    log_file: Path = project_root / "logs" / filename
    
    # Attach my two default observers
    logger.attach(ConsoleObserver())
    logger.attach(FileObserver(log_file))
    
    return logger
