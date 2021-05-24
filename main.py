from src.dataverse_writer import DataverseWriter, DatasetEntity, Dataset
from yaml import safe_load
import logging
import time


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

    #dv_dataset = Dataset()
    #dv_entity = DatasetEntity(datatype='image/png', data=open('image.png'), metadata={'filename': 'test.png', 'description': time.time()})
    #dv_entity.upload(api_endpoint='https://demo.dataverse.org', api_key='ac387b5c-0f41-4a75-9285-d88ca0db5446', persistent_id='doi:10.70122/FK2/73AJNM')
    #print(open('image.png'))