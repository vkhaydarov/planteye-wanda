import requests
import logging
import base64
import numpy as np
import cv2
from pyDataverse import api, models
import json
from time import time, sleep
import threading


class DataverseWriter:
    def __init__(self, cfg):
        self._dv_connection = None
        self.cfg = cfg
        self.dataset = None
        self._get_and_upload_thread = None
        self._stop = False
        pass

    def start(self):
        self._stop = False
        ret = self._connect_dataverse()
        if not ret:
            return
        ret = self._connect_to_dataset()
        if not ret:
            return
        self._receive_thread = threading.Thread(target=self._receive_data, args=[])
        self._receive_thread.start()

        self._upload_thread = threading.Thread(target=self._upload_data, args=[])
        self._upload_thread.start()

    def stop(self):
        self._stop = True
        sleep(self.cfg['dataverse']['read_upload_interval'] / 1000.0)

    def _connect_dataverse(self):
        self._dv_connection = api.NativeApi(self.cfg['dataverse']['endpoint'], self.cfg['dataverse']['token'])
        resp = self._dv_connection.get_info_version()
        print(resp.json())
        return True

    def _connect_to_dataset(self):
        self.dataset = Dataset(self.cfg['dataverse']['endpoint'], self.cfg['dataverse']['token'], self.cfg['dataverse']['dataset_pid'])
        return True

    def _create_dataset(self, data):
        data = {'citation_displayName': 'Citation Metadata', 'title': 'Youth in Austria 2005', 'author': [{'authorName': 'LastAuthor1, FirstAuthor1', 'authorAffiliation': 'AuthorAffiliation1'}], 'datasetContact': [{'datasetContactEmail': 'ContactEmail1@mailinator.com', 'datasetContactName': 'LastContact1, FirstContact1'}], 'dsDescription': [{'dsDescriptionValue': 'DescriptionText'}], 'subject': ['Medicine, Health and Life Sciences']}
        ds = models.Dataset(data=data)
        if not ds.validate_json():
            logging.error('Invalid data for dataset')
        resp = api.NativeApi.create_dataset(self._dv_connection, self.cfg['dataverse']['dataverse_name'], ds.json())
        print(resp)

    def _receive_data(self):

        # Set initial time point
        cycle_begin = time() - self.cfg['vision']['read_interval'] / 1000.0

        while not self._stop:

            logging.info('Receive data loop')

            # Calculate one cycle length
            cycle_begin = cycle_begin + self.cfg['vision']['read_interval'] / 1000.0

            # If last cycle lasted much longer, we need to skip the current polling cycle to catch up in the future
            if cycle_begin + 0.010 < time():
                logging.error('Reading skipped (consider increasing read interval)')
                continue

            begin_time = time()
            frame_received, frame_data = self._get_data_from_api()
            read_time = int((time() - begin_time) * 1000)

            if frame_received:
                logging.info('Frame received')
                begin_time = time()
                frame_bytes = base64.b64decode(frame_data['frame']['frame'])
                metadata = frame_data['metadata']
                timestamp = frame_data['timestamp']
                decode_time = int((time() - begin_time) * 1000)

                begin_time = time()
                data_entity = DatasetEntity(self.dataset, 'image/png', frame_bytes, metadata, timestamp)
                instance_time = int((time() - begin_time) * 1000)

                begin_time = time()
                self.dataset.add(data_entity)
                add_point_time = int((time() - begin_time) * 1000)
            else:
                logging.warning('Frame NOT received')
                decode_time = 0
                instance_time = 0
                add_point_time = 0

            total_time = int((time() - cycle_begin) * 1000)
            debug_str = 'Total execution time of receiving frame %i ms (data receiving %i, decoding %i, creating instance %i, adding to buffer %i)' \
                        % (total_time, read_time, decode_time, instance_time, add_point_time)
            logging.debug(debug_str)

            # Calculate real cycle duration
            cycle_dur = time() - cycle_begin

            # If the cycle duration longer than given and no connection issues, jump directly to the next cycle
            if cycle_dur > self.cfg['vision']['read_interval']:
                logging.warning('Reading takes longer ' + str(cycle_dur) + ' than given time intervals')
            else:
                # Calculate how long we need to wait till the begin of the next cycle
                sleep(max(self.cfg['vision']['read_interval'] / 1000.0 - (time() - cycle_begin), 0))

    def _upload_data(self):
        # Set initial time point
        cycle_begin = time() - self.cfg['dataverse']['upload_interval'] / 1000.0

        while not self._stop:

            logging.info('Upload loop')

            # Calculate one cycle length
            cycle_begin = cycle_begin + self.cfg['dataverse']['upload_interval'] / 1000.0

            # If last cycle lasted much longer, we need to skip the current polling cycle to catch up in the future
            if cycle_begin + 0.010 < time():
                logging.error('Uploading skipped (increase time interval)')
                continue

            begin_time = time()
            _, uploaded_items = self.dataset.upload()
            upload_time = int((time() - begin_time) * 1000)
            logging.info('Uploading skipped (increase time interval)')
            logging.debug('Total execution time of uploading %i frames %i ms' % (uploaded_items,  upload_time))

            # Calculate real cycle duration
            cycle_dur = time() - cycle_begin

            # If the cycle duration longer than given and no connection issues, jump directly to the next cycle
            if cycle_dur > self.cfg['dataverse']['upload_interval']:
                logging.warning('Uploading takes longer ' + str(cycle_dur) + ' than given time intervals')
            else:
                # Calculate how long we need to wait till the begin of the next cycle
                sleep(max(self.cfg['dataverse']['upload_interval'] / 1000.0 - (time() - cycle_begin), 0))

    def _get_data_from_api(self):
        api_endpoint = self.cfg['vision']['endpoint'] + 'get_frame'
        try:
            resp = requests.get(api_endpoint)
        except requests.exceptions.ConnectionError:
            logging.error('Cannot establish connection to ' + self.cfg['vision']['endpoint'])
            return False, None

        try:
            resp_data = resp.json()
        except Exception:
            logging.warning('Cannot deserialise received json %s' % resp_data)
            return False, None

        if resp_data['status']['code'] == 200:
            logging.info('Frame received')
            return True, resp_data
        else:
            logging.warning('No frame retrieved. Error %s API returned message %s'
                            % (resp_data['status']['code'], resp_data['status']['message']))
            return False, None


class Dataset:
    def __init__(self, api_endpoint, api_key, persistent_id, max_size=100):
        self.api_endpoint = api_endpoint
        self.api_key = api_key
        self.persistent_id = persistent_id
        self.entities = []
        self.max_size = max_size

    def add(self, new_entity):
        if len(self.entities) >= 100:
            self.entities.pop(0)
        self.entities.append(new_entity)

    def upload(self):
        ds_len = len(self.entities)
        if ds_len > 0:
            ds_uploaded = 0
            for entity in reversed(self.entities):
                res = entity.upload()
                if res:
                    self.entities.pop()
                    ds_uploaded += 1
            return True, ds_uploaded
        else:
            return False, 0


class DatasetEntity:
    def __init__(self, dataset, datatype, data, metadata, timestamp):
        self.dataset = dataset
        self.datatype = datatype
        self.data = data
        self.metadata = metadata
        self.timestamp = timestamp

    def _form_request_data(self):
        return dict(jsonData=json.dumps(self.metadata))

    def _form_request_file(self):
        if self.datatype == 'image/png':
            filename = 'frame%i.png' % self.timestamp
        return {'file': (filename, self.data, self.datatype)}

    def upload(self):
        api_endpoint = self.dataset.api_endpoint
        api_key = self.dataset.api_key
        persistent_id = self.dataset.persistent_id

        url_persistent_id = '%s/api/datasets/:persistentId/add?persistentId=%s&key=%s' % (api_endpoint, persistent_id, api_key)

        payload = self._form_request_data()
        files = self._form_request_file()
        begin_time = time()
        r = requests.post(url_persistent_id, data=payload, files=files)
        upload_time = int((time()-begin_time)*1000)
        logging.debug('Upload time of one frame %i ms' % upload_time)
        if r.status_code == 200:
            logging.info('Entity successfully uploaded on cloud')
            return True
        else:
            logging.warning('Entity could not be uploaded on cloud')
            return False

