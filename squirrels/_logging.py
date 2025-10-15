from pathlib import Path
from logging.handlers import RotatingFileHandler
from uuid import uuid4
import logging as l, json

from . import _constants as c, _utils as u


class _BaseFormatter(l.Formatter):
    def _format_helper(self, level_for_print: str, record: l.LogRecord) -> str:
        # Save original levelname
        original_levelname = record.levelname

        # Add padding to the levelname for printing
        visible_length = len(record.levelname) + 1
        padding_needed = max(1, 9 - visible_length)
        padded_level = f"{level_for_print}:{' ' * padding_needed}"
        record.levelname = padded_level
        
        # Format the message
        formatted = super().format(record)
        
        # Restore original levelname
        record.levelname = original_levelname
        
        return formatted


class _ColoredFormatter(_BaseFormatter):
    """Custom formatter that adds colors to log levels for terminal output"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    def format(self, record: l.LogRecord) -> str:
        # Add color to levelname with colon and padding
        color = self.COLORS.get(record.levelname, '')
        colored_level = f"{color}{record.levelname}{self.RESET}"
        return self._format_helper(colored_level, record)


class _PlainFormatter(_BaseFormatter):
    """Custom formatter that adds colon to log levels for file output"""
    
    def format(self, record: l.LogRecord) -> str:
        return self._format_helper(record.levelname, record)


class _CustomJsonFormatter(l.Formatter):
    def format(self, record: l.LogRecord) -> str:
        super().format(record)
        info = {
            "timestamp": self.formatTime(record),
            "project_id": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
            "thread": record.thread,
            "thread_name": record.threadName,
            "process": record.process,
            **record.__dict__.get("info", {})
        }
        output = {
            "data": record.__dict__.get("data", {}),
            "info": info
        }
        return json.dumps(output)


def get_logger(
    base_path: str, log_to_file: bool, log_level: str, log_format: str, log_file_size_mb: int, log_file_backup_count: int
) -> u.Logger:
    logger = u.Logger(name=uuid4().hex)
    logger.setLevel(log_level.upper())

    # Determine the formatter based on log_format
    if log_format.lower() == "json":
        stdout_formatter = _CustomJsonFormatter()
        file_formatter = _CustomJsonFormatter()
    elif log_format.lower() == "text":
        # Use colored formatter for stdout, plain formatter with colon for file
        format_string = "%(levelname)s [%(asctime)s] %(message)s"
        stdout_formatter = _ColoredFormatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
        file_formatter = _PlainFormatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
    else:
        raise ValueError("log_format must be either 'text' or 'json'")
    
    # Always add stdout handler at the configured log level
    stdout_handler = l.StreamHandler()
    stdout_handler.setLevel(log_level.upper())
    stdout_handler.setFormatter(stdout_formatter)
    logger.addHandler(stdout_handler)
    
    # Optionally add rotating file handler with retention policies
    if log_to_file:
        log_file_path = Path(base_path, c.LOGS_FOLDER, c.LOGS_FILE)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Rotating file handler: max 10MB per file, keep 5 backup files
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=log_file_size_mb * 1024 * 1024,
            backupCount=log_file_backup_count
        )
        file_handler.setLevel(log_level.upper())
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger
