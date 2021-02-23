import os
import sys
import time
import logging
from time import strftime, gmtime


log = logging.getLogger('Blob Migration')
_date_time = strftime("%Y%m%d_%H%M%S", gmtime())

_format = '[%(process)d] %(asctime)s %(levelname)5s %(name)-20s %(message)s'

log.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter(_format))
log.addHandler(console_handler)

filename = f"DEBUG_{_date_time}.log"
file_handler = logging.FileHandler(filename)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(_format))
log.addHandler(file_handler)
