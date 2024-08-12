import logging, coloredlogs, json
from uuid import uuid4
from functools import wraps
from .env import get

coloredlogs.CAN_USE_BOLD_FONT = True
coloredlogs.DEFAULT_FIELD_STYLES = {
    "asctime": {"color": "white"},
    "hostname": {"color": "magenta"},
    "levelname": {"color": "green", "bold": True},
    "name": {"color": "blue"},
    "programname": {"color": "cyan"},
    "pathname": {"color": "blue"},
    "funcName": {"color": "black"},
    "module": {"color": "green"},
    "lineno": {"color": "blue"},
}
coloredlogs.DEFAULT_LEVEL_STYLES = {
    "critical": {"color": "red", "bold": True},
    "error": {"color": "red"},
    "warning": {"color": "yellow"},
    "notice": {"color": "magenta"},
    "info": {"color": "white"},
    "debug": {"color": "green"},
    "spam": {"color": "green", "faint": True},
    "success": {"color": "green", "bold": True},
    "verbose": {"color": "blue"},
}


class Logg:
    logger: logging.Logger
    request_id: str

    def __init__(self, name: str = "log"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level=get("LOG_LEVEL"))
        format_str = (
            "%(levelname)s: %(asctime)s %(pathname)s "
            "%(funcName)s %(module)s Line%(lineno)d %(message)s"
        )
        coloredlogs.install(logger=self.logger, fmt=format_str)
        self.request_id = self.generate_id()

    def log(self, lebel: int, message: dict, stacklevel: int = 2):
        message = {k: str(v) for k, v in message.items()}
        message["request_id"] = self.request_id
        self.logger.log(level=lebel, msg=json.dumps(message), stacklevel=stacklevel)

    def info(self, title: str, message: dict = {}, stacklevel: int = 3):
        message["title"] = title
        self.log(logging.INFO, message, stacklevel=stacklevel)

    def debug(self, title: str, message: dict = {}, stacklevel: int = 3):
        message["title"] = title
        self.log(logging.DEBUG, message, stacklevel=stacklevel)

    def critical(self, title: str, message: dict = {}, stacklevel: int = 3):
        message["title"] = title
        self.log(logging.CRITICAL, message, stacklevel=stacklevel)

    def error(self, title: str, message: dict = {}, stacklevel: int = 3):
        message["title"] = title
        self.log(logging.ERROR, message, stacklevel=stacklevel)

    def warning(self, title: str, message: dict = {}, stacklevel: int = 3):
        message["title"] = title
        self.log(logging.WARNING, message, stacklevel=stacklevel)

    @staticmethod
    def generate_id():
        return str(uuid4())

    def start(self, method_name: str, stacklevel: int = 4):
        self.info(f"start {method_name}", stacklevel=stacklevel)

    def end(self, method_name: str, stacklevel: int = 4):
        self.info(f"end {method_name}", stacklevel=stacklevel)


def output_start_end(resolver):
    @wraps(resolver)
    def wrapper(*args, **kwargs):
        cl = args[0]
        logg: Logg = cl.logg
        logg.start(resolver.__name__)

        result = resolver(*args, **kwargs)
        logg.end(resolver.__name__)
        return result

    return wrapper
