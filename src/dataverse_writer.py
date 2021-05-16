import requests
import logging
import base64
import numpy as np
import cv2
from pyDataverse import api, models


class DataverseWriter:
    def __init__(self, cfg):
        self._dv_connection = None
        self.cfg = cfg
        pass

    def start(self):
        self._connect_dataverse()

    def stop(self):
        pass

    def _connect_dataverse(self):
        self._dv_connection = api.NativeApi(self.cfg['dataverse']['endpoint'], self.cfg['dataverse']['token'])
        resp = self._dv_connection.get_info_version()
        print(resp.json())

    def _create_dataset(self, data):
        data = {'citation_displayName': 'Citation Metadata', 'title': 'Youth in Austria 2005', 'author': [{'authorName': 'LastAuthor1, FirstAuthor1', 'authorAffiliation': 'AuthorAffiliation1'}], 'datasetContact': [{'datasetContactEmail': 'ContactEmail1@mailinator.com', 'datasetContactName': 'LastContact1, FirstContact1'}], 'dsDescription': [{'dsDescriptionValue': 'DescriptionText'}], 'subject': ['Medicine, Health and Life Sciences']}
        ds = models.Dataset(data=data)
        if not ds.validate_json():
            logging.error('Invalid data for dataset')
        resp = api.NativeApi.create_dataset(self._dv_connection, self.cfg['dataverse']['dataverse_name'], ds.json())
        print(resp)

    def _get_data_from_api(self):
        api_endpoint = self.cfg['api']['endpoint'] + '/get_frame'
        try:
            response = requests.get(api_endpoint)
        except requests.exceptions.ConnectionError:
            logging.error('Cannot establish connection to ' + self.cfg['api']['endpoint'])
            return None

        resp_data = response.json()

        if resp_data['status']['code'] == 500:
            logging.warning('No frame retrieved. API returned message: ' + resp_data['status']['message'])
            return None

        if resp_data['status']['code'] == 200:
            logging.info('Frame received')

        return resp_data

    def _start_upload(self):
        pass

    def _stop_upload(self):
        pass

    def _upload(self):
        pass


class Dataset:
    def __init__(self):
        pass

    def _add__(self):
        pass

    def __del__(self):
        pass

    def _convert_str_to_png(self):
        pass


def convert_str_to_frame(frame_str):
    # https://jdhao.github.io/2020/03/17/base64_opencv_pil_image_conversion/

    frame_bytes = base64.b64decode(frame_str)
    frame_arr = np.frombuffer(frame_bytes, dtype=np.uint8)  # im_arr is one-dim Numpy array
    frame = cv2.imdecode(frame_arr, flags=cv2.IMREAD_COLOR)
    return frame

