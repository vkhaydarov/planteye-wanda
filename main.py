from src.dataverse_writer import DataverseWriter
from yaml import safe_load
import logging


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s.%(msecs)03d [%(levelname)s] %(module)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

    # Import and validate config
    with open('config.yaml') as config_file:
        cfg = safe_load(config_file)
    #validation_res, validation_msg = validate_cfg(cfg, 'src/config_schema.json')

    #if not validation_res:
    #    print(validation_msg)
    #   exit(1)
    dv_writer = DataverseWriter(cfg)
    dv_writer.start()
