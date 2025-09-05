from datetime import datetime
import logging
import os


async def setup_logger(name = 'scraper', log_dir = 'logs', level = logging.INFO):
    os.makedirs(log_dir, exist_ok = True)
    log_filename = os.path.join(log_dir, f'{name}_{datetime.now().strftime("%Y-%m-%d")}.log')
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    
    fh = logging.FileHandler(log_filename)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger
    
    