from pymongo import MongoClient
import certifi
from cogs._config import db_url
import pickle
import threading
import os
from discord.ext import commands
import json
from main import logger


ca = certifi.where()
client = MongoClient(db_url,tls=True,tlsCAFile=ca)

db = client['gdriveclonebot']
gdrivecreds = db['gdrive_creds']
parentids = db['parent_ids']
sas_db = db['sas']
sascre_db = db['sas_cre']


INSERTION_LOCK = threading.RLock()
################
def insert_creds(user_id,cred_str):
    with INSERTION_LOCK:
        data = {
            "user_id" : user_id,
            "cred_str":pickle.dumps(cred_str)
        }
        if current_cred := gdrivecreds.find_one({"user_id": user_id}):
            gdrivecreds.update_one(current_cred,{"$set":data})
        else:
            gdrivecreds.insert_one(data)

def find_creds(user_id):
    with INSERTION_LOCK:
        if cred := gdrivecreds.find_one({"user_id": user_id}):
            return pickle.loads(cred['cred_str'])
        else:
            return None

def delete_creds(user_id):
    with INSERTION_LOCK:
        gdrivecreds.delete_one({"user_id":user_id})


###############

def sascre_insert_creds(user_id,cred_str):
    with INSERTION_LOCK:
        data = {
            "user_id" : user_id,
            "cred_str":pickle.dumps(cred_str)
        }
        if current_cred := sascre_db.find_one({"user_id": user_id}):
            sascre_db.update_one(current_cred,{"$set":data})
        else:
            sascre_db.insert_one(data)

def sascre_find_creds(user_id):
    with INSERTION_LOCK:
        if cred := sascre_db.find_one({"user_id": user_id}):
            return pickle.loads(cred['cred_str'])
        else:
            return None

def sascre_delete_creds(user_id):
    with INSERTION_LOCK:
        sascre_db.delete_one({"user_id":user_id})


##############
def insert_parent_id(user_id,parent_id):
    data = {
        "user_id" : user_id,
        "parent_id":parent_id
    }
    if current_parent_id := parentids.find_one({"user_id": user_id}):
        parentids.update_one(current_parent_id,{"$set":data})
    else:
        parentids.insert_one(data)

def find_parent_id(user_id):
    if parent_id := parentids.find_one({"user_id": user_id}):
        return parent_id['parent_id']
    else:
        return None

def delete_parent_id(user_id):
    parentids.delete_one({"user_id":user_id})

############
def upload_sas():
    sas_folder_parent = f"{os.getcwd()}/sas"
    dirlist = os.listdir(sas_folder_parent)
    sas_folder = f"{sas_folder_parent}/{dirlist[0]}"
    sa_files = os.listdir(sas_folder)
    for idx,filename in enumerate(sa_files):
        with open(f"{sas_folder}/{filename}") as f:
            data = json.load(f)
            data['sa_file_index'] = idx
            sas_db.insert_one(data)

def find_sas():
    return list(sas_db.find())

def delete_sas():
    sas_db.drop()

def find_sa_info_by_id(id):
    return sas_db.find_one({"sa_file_index":id})

###################################

def create_db_insert_sas(project_id):
    temp = db[f'sas_{project_id}']
    sas_folder = 'accounts'
    sa_files = os.listdir(sas_folder)
    for filename in sa_files:
        with open(f"{sas_folder}/{filename}") as f:
            data = json.load(f)
            temp.insert_one(data)

def download_sas_projid(project_id):
    temp = db[f'sas_{project_id}']
    sas_ls = list(temp.find({},{"_id":0}))
    if not os.path.exists('accounts'):
        os.mkdir('accounts')
    for j in sas_ls:
        with open(f'accounts/{j["private_key_id"]}.json', 'w+') as f:
            f.write(json.dumps(j))

def sas_for_projid_exists(project_id):
    temp = db[f'sas_{project_id}']
    sas_ls = list(temp.find())
    return len(sas_ls) != 0



####### ================= ######### checks

def has_credentials():
    async def predicate(ctx:commands.Context):
        return bool(find_creds(ctx.author.id))

    return commands.check(predicate)

def has_sa_creds():
    async def predicate(ctx:commands.Context):
        return bool(sascre_find_creds(ctx.author.id))

    return commands.check(predicate)


def not_has_credentials():
    async def predicate(ctx:commands.Context):
        return not find_creds(ctx.author.id)

    return commands.check(predicate)

def not_has_sa_creds():
    async def predicate(ctx:commands.Context):
        return not sascre_find_creds(ctx.author.id)

    return commands.check(predicate)

def has_uploaded_sas():
    async def predicate(ctx):
        x = sas_db.find_one()
        return bool(x)

    return commands.check(predicate)
