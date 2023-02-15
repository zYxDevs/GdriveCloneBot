import time
from base64 import b64decode
from random import choice
import os
import traceback
from main import logger
from googleapiclient.discovery import build

import cogs._db_helpers as db

SCOPES = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/cloud-platform','https://www.googleapis.com/auth/iam']

def _generate_id(prefix='saf-'):
    chars = '-abcdefghijklmnopqrstuvwxyz1234567890'
    return prefix + ''.join(choice(chars) for _ in range(25)) + choice(chars[1:])


def _list_sas(iam,project):
    resp = (
        iam.projects()
        .serviceAccounts()
        .list(name=f'projects/{project}', pageSize=100)
        .execute()
    )
    return resp['accounts'] if 'accounts' in resp else []

class ServAcc:
    def __init__(self,user_id):
        self.user_id = user_id
        self.scopes = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/cloud-platform','https://www.googleapis.com/auth/iam','https://www.googleapis.com/auth/service.management']
        self.sleep_time = 30
        self.creds = db.sascre_find_creds(self.user_id)
        self._services = self.authorise()
        self.services=['iam','drive','serviceusage','cloudresourcemanager']
        self.current_key_dump = []
    
    def authorise(self):
        self.cloud = build('cloudresourcemanager', 'v1', credentials=self.creds)
        self.iam = build('iam', 'v1', credentials=self.creds)
        self.serviceusage = build('serviceusage','v1',credentials=self.creds)
        return [self.cloud,self.iam,self.serviceusage]

    def _list_projects(self) -> list:
        return [i['projectId'] for i in self.cloud.projects().list().execute()['projects']]

    def enableservices(self,projectid) -> None:
        services_links = [f'{i}.googleapis.com' for i in self.services]
        projects = [projectid]
        batch = self.serviceusage.new_batch_http_request(callback=self._def_batch_resp)
        for i in projects:
            for j in services_links:
                batch.add(
                    self.serviceusage.services().enable(
                        name=f'projects/{i}/services/{j}'
                    )
                )
        batch.execute()
    
    def createsas(self,projectid):
        sa_count = len(_list_sas(self.iam,projectid))
        while sa_count != 100:
            self._create_accounts(self.iam,projectid,100 - sa_count)
            sa_count = len(_list_sas(self.iam,projectid))

    def _create_accounts(self,service,project,count):
        batch = service.new_batch_http_request(callback=self._def_batch_resp)
        for _ in range(count):
            aid = _generate_id('mfc-')
            batch.add(
                service.projects()
                .serviceAccounts()
                .create(
                    name=f'projects/{project}',
                    body={
                        'accountId': aid,
                        'serviceAccount': {'displayName': aid},
                    },
                )
            )
        batch.execute()

    def _def_batch_resp(self,id,resp,exception):
        if exception is not None:
            if str(exception).startswith('<HttpError 429'):
                time.sleep(self.sleep_time/100)
            else:
                print(exception)

    def download_keys(self,projectid):
        self._create_sa_keys(self.iam,[projectid],'accounts')
        

    def _create_sa_keys(self,iam,projects,path):
        all_projs = self._list_projects()
        all_good = False
        for proj_id in projects:
            if proj_id not in all_projs:
                raise Exception(f"Error: Project id {proj_id} not found, All projects = {all_projs}")
        try:
            if not os.path.exists(path):
                os.makedirs(path)
            for i in projects:
                self.current_key_dump = []
                while self.current_key_dump is None or len(self.current_key_dump) != 100:
                    batch = iam.new_batch_http_request(callback=self._batch_keys_resp)
                    total_sas = _list_sas(iam,i)
                    for j in total_sas:
                        batch.add(
                            iam.projects()
                            .serviceAccounts()
                            .keys()
                            .create(
                                name=f"projects/{i}/serviceAccounts/{j['uniqueId']}",
                                body={
                                    'privateKeyType': 'TYPE_GOOGLE_CREDENTIALS_FILE',
                                    'keyAlgorithm': 'KEY_ALG_RSA_2048',
                                },
                            )
                        )
                    print("DOWNLOADING KEYSS !!!!!!")
                    logger.critical('DOWNLOADING KEYSS !!!!!!')

                    batch.execute()
                    print("DOWNLOADED KEYSS !!!!!!")
                    logger.critical('DOWNLOADED KEYSS !!!!!!')
                    if self.current_key_dump is None:
                        self.current_key_dump = []
                    else:
                        for j in self.current_key_dump:
                            with open(f'{path}/{j[0]}.json', 'w+') as f:
                                f.write(j[1])
        except Exception as e:
            print(e)
            logger.error(e,exc_info=True)
            traceback.print_exc()

    def _batch_keys_resp(self,id,resp,exception):
        if exception is not None:
            self.current_key_dump = None
            time.sleep(self.sleep_time/100)
        elif self.current_key_dump is None:
            time.sleep(self.sleep_time/100)
        else:
            self.current_key_dump.append((
                resp['name'][resp['name'].rfind('/'):],
                b64decode(resp['privateKeyData']).decode('utf-8')
            ))




