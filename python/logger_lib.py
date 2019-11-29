import logging
import sys

def setup_logger(logger_filename, loglevel="INFO", file_loglevel="INFO", name='log'):
    """ Set up logging facility """
    
    # Setup the logger
    
    
    # instanciate the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Filehandlier
    form_File = logging.Formatter('%(asctime)s - %(module)s - %(funcName)s - %(lineno)s - %(levelname)s - '
                                  '%(message)s')
    fh = logging.FileHandler(logger_filename)
    #fh.setLevel(logging.DEBUG)

    # If SAME, use the same loglevel as VERBOSE for file_loglevel
    if file_loglevel == "SAME":
        file_loglevel = loglevel

    if not file_loglevel in ["DEBUG", "INFO", "WARNING", "ERROR", "SAME"]:
        logger.error("Error : wrong log level : ", loglevel)
        sys.exit(1)
    if file_loglevel == "DEBUG":
        fh.setLevel(logging.DEBUG)
    elif file_loglevel == "INFO":
        fh.setLevel(logging.INFO)
    elif file_loglevel == "WARNING":
        fh.setLevel(logging.WARNING)
    elif file_loglevel == "ERROR":
        fh.setLevel(logging.ERROR)
    else:
        logger.error("Error : wrong log level")
        sys.exit(1)
    fh.setFormatter(form_File)

    # ConsoleHandler
    ch = logging.StreamHandler()
    form_Console = logging.Formatter('%(module)s - %(message)s')
    ch.setFormatter(form_Console)

    # Get the log level
    if not loglevel in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        logger.error("Error : wrong log level : ", loglevel)
        sys.exit(1)
    if loglevel == "DEBUG":
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(form_File)
    elif loglevel == "INFO":
        ch.setLevel(logging.INFO)
    elif loglevel == "WARNING":
        ch.setLevel(logging.WARNING)
    elif loglevel == "ERROR":
        ch.setLevel(logging.ERROR)
    else:
        logger.error("Error : wrong log level")
        sys.exit(1)

    # Add Handlers
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger