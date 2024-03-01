import logging, pytz, sys, re, io
from datetime import datetime
import functools

def timetz(*args):
    tz = pytz.timezone('Asia/Singapore')
    return datetime.now(tz).timetuple()

def addLoggingLevel(levelName, levelNum, methodName=None):
    """

    Source : https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility

    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    #>>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    #>>> logging.getLogger(__name__).setLevel("TRACE")
    #>>> logging.getLogger(__name__).trace('that worked')
    #>>> logging.trace('so did this')
    #>>> logging.TRACE
    5

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
       raise AttributeError('{} already defined in logging module'.format(levelName))
    if hasattr(logging, methodName):
       raise AttributeError('{} already defined in logging module'.format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
       raise AttributeError('{} already defined in logger class'.format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)
    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)

class Logger():

    def __init__(self, _name,**kwargs):
        # Add custom logging with levels (Higher number take precedence)
        #Logging levels for reference
        # NOTSET = 0
        # DEBUG = 10
        # INFO = 20
        # WARN = 30
        # ERROR = 40
        # CRITICAL = 50

        self.logger = logging.getLogger(_name)
        self.logger.setLevel(logging.ERROR)
        self.logger.propagate = False

        if 'stream' in kwargs.keys():
            # 1 stream handler per logger
            self.stream = io.StringIO()
            ch = logging.StreamHandler(self.stream)
            logging.Formatter.converter = timetz
            formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s',"%Y-%m-%d %H:%M:%S")
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        if 'file' in kwargs.keys():
            # Multiple File Handlers
            for filename in kwargs['file']:
                file_handler = logging.FileHandler(filename)
                logging.Formatter.converter = timetz
                formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s', "%Y-%m-%d %H:%M:%S")
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)

    def flush(self):
        for handler in self.handlers:
            handler.flush

def add_logging_level(text, level_number, function_name):
    if text not in dir(logging):
        addLoggingLevel(text, level_number, function_name)

# Manually add level to override other log types
add_logging_level('LOG',55,'custom_logs')


# Create logging decorator
def log_fuction(func=None,logger=None):
    rootlogger=logger
    def _log_decorater(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            rootlogger.logger.info(f'Calling function : {func.__name__}')
            try:
                result = func(*args, **kwargs)
                rootlogger.logger.custom_logs(f'Function {func.__name__} completed successfully for arguments {args}')
                print(f'Function {func.__name__} completed successfully for arguments {args}')
                return result
            except Exception as e:
                rootlogger.logger.error(f'Function {func.__name__} failed with error for arguments {args}: {str(e)}')
                print(f'Function {func.__name__} failed with error for arguments {args}: {str(e)}')
                # raise
        return wrapper
    return _log_decorater(func) if func else _log_decorater