import logging

__all__ = ("logger", "setup_logging")

logger = logging.getLogger("dagger")


def setup_logging(level: str) -> None:
    level = level.upper()
    format_str = "[%(asctime)s - %(pathname)s:%(lineno)d - %(levelname)s/%(process)s] %(message)s"
    logging.basicConfig(format=format_str)
    logger.setLevel(level)
