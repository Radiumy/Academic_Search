import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
import os


def configure_logging(save_debug: bool = False):
    """Configure logging: file handler (DEBUG or INFO) + console handler (WARNING)

    Args:
        save_debug: If True, file handler uses DEBUG level. If False, uses INFO level.
    """
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create timestamped log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"crawler_{timestamp}.log"

    # File handler: DEBUG or INFO level, detailed format
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG if save_debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))

    # Console handler: WARNING level, simplified format
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s - %(message)s'
    ))

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Configure crawl4ai logger to ensure it propagates to root
    crawl4ai_logger = logging.getLogger("crawl4ai")
    crawl4ai_logger.setLevel(logging.DEBUG if save_debug else logging.INFO)
    crawl4ai_logger.propagate = True

    # Create symlink to latest log file
    latest_link = log_dir / "latest.log"
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    try:
        os.symlink(log_file.name, latest_link)
    except OSError:
        # Symlink may fail on some systems (e.g., Windows), ignore
        pass
