import time, os, PTN, qbittorrentapi, re, requests
import pandas as pd
from imdb import Cinemagoer
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from huggingface_hub import HfApi

token = os.getenv('token')
res = requests.get("https://raw.githubusercontent.com/piyushpradhan22/credentials/refs/heads/main/credentials.json",
                   headers={"Authorization" : f"token {token}"}).json()

username = res['username']
password = res['password']

# Connection details
conn_info = {'host': 'localhost', 'port': 7860, 'username': username, 'password': password}

# Instantiate the client
qbt_client = qbittorrentapi.Client(**conn_info)
qbt_client.auth_log_in()

postgres_engine = create_engine(res['postgres_url'], poolclass=NullPool)
ia = Cinemagoer()
hf_token = res['hf_token']

# API instance
api = HfApi(token=hf_token)
repo_id = res['repo_id']
repo_type = "dataset"

def list_video_files(directory, hash):
    video_files = []
    i = 1
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.mkv') or file.endswith('.mp4'):
                file_path = os.path.join(root, file)
                file_name = file_path.split("/")[-1]
                video_files.append({"file_path" : file_path, "size" : os.path.getsize(file_path),
                                    "file_hash" : f"{hash}_{i}" })
                i+=1
    return video_files

# List of characters to be replaced
disallowed_chars = ['—']

def replace_disallowed_chars(folder_name):
    # Create a regex pattern that matches any of the disallowed characters
    pattern = re.compile('|'.join(map(re.escape, disallowed_chars)))
    # Replace matched characters with a dot
    return pattern.sub('.', folder_name)

torrs = [tor for tor in qbt_client.torrents_info() if tor.progress==1 and tor.state != 'pausedUP']
print("Received Torrent" , [tor.name for tor in torrs])
hashes = [tor.hash for tor in torrs]
qbt_client.torrents_pause(hashes)
for tor in torrs:
    #save_path = replace_disallowed_chars(tor['content_path'])
    save_path = tor['content_path']
    if save_path.split(".")[-1] in ['mkv', 'mp4']:
        video_files = [{"file_path" : save_path, "size" : os.path.getsize(save_path),
                                    "file_hash" : tor.hash}]
    else:
        video_files = list_video_files(save_path, tor.hash)
        
    if not video_files:
        print("No video files found...")
    if tor.name[:5] == 'imdb:':
        imdb_id = tor.name.split(":")[1]
        tor.name = tor.name.split(":")[2]
    else:
        imdb_id = None
    for vd in video_files:
        file_path = vd['file_path']
        file_name = file_path.split("/")[-1]
        if 'sample' in file_name.lower():
            continue
        file_hash = vd['file_hash']
        # Upload to HF Dataset
        message = api.upload_file(path_or_fileobj=file_path, path_in_repo=file_hash, repo_id=repo_id, repo_type=repo_type,)
        print(message)
        ptn = PTN.parse(file_name)
        imdb = ia.search_movie(PTN.parse(tor.name)['title']) if 'season' in ptn.keys() else ia.search_movie(ptn['title'])
        imdb = imdb if imdb else ia.search_movie(PTN.parse(tor.name)['title'])
        imdb = imdb if imdb else ia.search_movie(ptn['title'])
        if not imdb_id:
            imdb_id = 'tt'+imdb[0].movieID if imdb else ''
            imdb_id = f"tt{imdb[0].movieID}:{ptn['season']}:{ptn['episode']}" if 'episode' in ptn.keys() else imdb_id
        else:
            imdb_id = f"{imdb_id}:{ptn['season']}:{ptn['episode']}" if 'episode' in ptn.keys() else imdb_id
        server_url = f"https://huggingface.co/datasets/{repo_id}/resolve/main/{file_hash}?download=true"
        
        df = pd.DataFrame([{"imdb_id": imdb_id, 'name' : tor.name, 'file_name' : file_name, "url" : server_url, "size" : vd["size"], "time" : time.time(), "hash" : tor.hash}])
        try:
            df.to_sql(name='hftor', con=postgres_engine, if_exists='append', index=False)
        except:
            pass
    qbt_client.torrents_delete(delete_files=True, torrent_hashes=tor.hash)