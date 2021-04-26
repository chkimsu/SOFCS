import os
import config.file_path as fp
import utils.marker as mk

if not os.path.exists(fp.run_dir() + "file_handler.run"):
    os.system('python3 file_handler.py --id 1 --log debug &')
else:
    mk.debug_info("file_handler.py is already running. Check your process.", m_type="WARNING")

if not os.path.exists(fp.run_dir() + "output_handler.run"):
    os.system('python3 output_handler.py --id 1 --log debug &')
else:
    mk.debug_info("output_handler.py is already running. Check your process.", m_type="WARNING")
