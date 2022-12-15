from logging import config, DEBUG, Formatter, getLogger, StreamHandler, WARN

import yaml


def init_logger(cfg_path: str):
    try:
        conf_dict = yaml.safe_load(open(cfg_path).read())
        config.dictConfig(conf_dict)

    except Exception as e:
        handler = StreamHandler()
        handler.setLevel(WARN)

        fmt = '%(levelname)-8s: %(asctime)s | %(message)s'
        formatter = Formatter(
            fmt,
            datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)

        logger = getLogger()
        logger.setLevel(DEBUG)
        logger.addHandler(handler)

        logger.warn('Failed to load log config.')
        logger.warn(e)
