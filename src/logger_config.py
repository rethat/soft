import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

class SizeTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    Custom handler that rotates based on both time (daily) and size (10MB)
    """
    def __init__(self, filename, max_bytes=10*1024*1024, when='midnight', interval=1, backupCount=0, encoding=None, delay=False, utc=False, atTime=None):
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(filename)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Add date to filename
        base_name = os.path.splitext(filename)[0]
        ext = os.path.splitext(filename)[1] or '.log'
        date_str = datetime.now().strftime('%Y-%m-%d')
        dated_filename = f"{base_name}_{date_str}{ext}"
        
        super().__init__(dated_filename, when=when, interval=interval, backupCount=backupCount, encoding=encoding, delay=delay, utc=utc, atTime=atTime)
        self.max_bytes = max_bytes
        self.base_filename = base_name
        self.ext = ext
    
    def shouldRollover(self, record):
        """
        Determine if rollover should occur based on both time and size
        """
        # Check time-based rollover first
        if super().shouldRollover(record):
            return True
        
        # Check size-based rollover
        if self.stream is None:
            self.stream = self._open()
        
        if self.max_bytes > 0:
            msg = "%s\n" % self.format(record)
            self.stream.seek(0, 2)  # Seek to end of file
            if self.stream.tell() + len(msg) >= self.max_bytes:
                return True
        
        return False
    
    def doRollover(self):
        """
        Perform rollover when either time or size threshold is reached
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        
        # Get current time for new filename
        current_time = datetime.now()
        date_str = current_time.strftime('%Y-%m-%d')
        
        # Create new filename with date and sequence number if needed
        new_filename = f"{self.base_filename}_{date_str}{self.ext}"
        
        # If file exists, add sequence number
        if os.path.exists(new_filename):
            counter = 1
            while os.path.exists(f"{self.base_filename}_{date_str}_{counter}{self.ext}"):
                counter += 1
            new_filename = f"{self.base_filename}_{date_str}_{counter}{self.ext}"
        
        self.baseFilename = new_filename
        
        # Clean up old log files if backup count is set
        if self.backupCount > 0:
            self._cleanup_old_logs()
        
        if not self.delay:
            self.stream = self._open()
    
    def _cleanup_old_logs(self):
        """
        Remove old log files that exceed backup count
        """
        import glob
        from datetime import timedelta
        
        # Get all log files matching the pattern
        log_dir = os.path.dirname(self.base_filename) or '.'
        pattern = os.path.join(log_dir, f"{os.path.basename(self.base_filename)}_*{self.ext}")
        log_files = glob.glob(pattern)
        
        # Sort by modification time (oldest first)
        log_files.sort(key=lambda x: os.path.getmtime(x))
        
        # Remove files that exceed backup count
        if len(log_files) > self.backupCount:
            files_to_remove = log_files[:-self.backupCount]
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                except OSError:
                    pass

def setup_logging(log_dir='logs', log_level=logging.INFO):
    """
    Setup logging configuration with all levels, daily rotation, and 10MB size limit
    
    Args:
        log_dir: Directory to store log files
        log_level: Minimum logging level (default: DEBUG to capture all levels)
    
    Returns:
        logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler with date and size rotation
    log_file = os.path.join(log_dir, 'app.log')
    file_handler = SizeTimedRotatingFileHandler(
        filename=log_file,
        max_bytes=10*1024*1024,  # 10MB
        when='midnight',  # Rotate at midnight
        interval=1,  # Daily
        backupCount=30,  # Keep 30 days of logs
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler (optional - for development)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def get_logger(name=None):
    """
    Get a logger instance with the specified name
    
    Args:
        name: Logger name (usually __name__)
    
    Returns:
        logger: Logger instance
    """
    return logging.getLogger(name)

