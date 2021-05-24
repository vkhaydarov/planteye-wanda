import requests
import logging
import base64
import numpy as np
import cv2
from pyDataverse import api, models
import json
import time
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
        self._get_and_upload_thread = threading.Thread(target=self._get_and_upload_data, args=[])
        self._get_and_upload_thread.start()

    def stop(self):
        self._stop = True
        time.sleep(self.cfg['dataverse']['read_upload_interval'] / 1000.0)

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

    def _get_and_upload_data(self):

        # Set initial time point
        cycle_begin = time.time() - self.cfg['dataverse']['read_upload_interval'] / 1000.0

        while not self._stop:

            logging.info('Loop step')

            # Calculate one cycle length
            cycle_begin = cycle_begin + self.cfg['dataverse']['read_upload_interval'] / 1000.0

            # If last cycle lasted much longer, we need to skip the current polling cycle to catch up in the future
            if cycle_begin + 0.010 < time.time():
                logging.error('Reading and uploading skipped (increase time interval)')
                continue

            received_data = self._get_data_from_api()
            frame_bytes = base64.b64decode(received_data['frame']['frame'])
            metadata = received_data['metadata']
            timestamp = received_data['timestamp']

            data_entity = DatasetEntity(self.dataset, 'image/png', frame_bytes, metadata, timestamp)
            self.dataset.add(data_entity)
            self.dataset.upload()

            if self._stop:
                break

            # Calculate real cycle duration
            cycle_dur = time.time() - cycle_begin

            # If the cycle duration longer than given and no connection issues, jump directly to the next cycle
            if cycle_dur > self.cfg['dataverse']['read_upload_interval']:
                logging.warning('Reading and uploading takes longer ' + str(cycle_dur) + ' than given time intervals')
            else:
                # Calculate how long we need to wait till the begin of the next cycle
                time.sleep(max(self.cfg['dataverse']['read_upload_interval'] / 1000.0 - (time.time() - cycle_begin), 0))

    def _get_data_from_api(self):
        api_endpoint = self.cfg['vision']['endpoint'] + '/get_frame'
        try:
            response = requests.get(api_endpoint)
        except requests.exceptions.ConnectionError:
            logging.error('Cannot establish connection to ' + self.cfg['vision']['endpoint'])
            return None

        resp_data = response.json()

        if resp_data['status']['code'] == 500:
            logging.warning('No frame retrieved. API returned message: ' + resp_data['status']['message'])
            return None

        if resp_data['status']['code'] == 200:
            logging.info('Frame received')

        return resp_data


class Dataset:
    def __init__(self, api_endpoint, api_key, persistent_id):
        self.api_endpoint = api_endpoint
        self.api_key = api_key
        self.persistent_id = persistent_id
        self.entities = []

    def add(self, new_entity):
        self.entities.append(new_entity)

    def upload(self):
        for entity in reversed(self.entities):
            res = entity.upload()
            if res:
                self.entities.pop()


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

        #print('-' * 40)
        #print('making request: %s' % url_persistent_id)
        r = requests.post(url_persistent_id, data=payload, files=files)
        #print(payload)
        #print(files)
        #print('-' * 40)
        #print(r.json())
        #print(r.status_code)
        if r.status_code == 200:
            logging.info('Entity successfully uploaded on cloud')
            return True
        else:
            logging.warning('Entity could not be uploaded on cloud')
            return False

