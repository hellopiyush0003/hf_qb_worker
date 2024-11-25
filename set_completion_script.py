import qbittorrentapi, os, requests

token = os.getenv('token')
res = requests.get("https://raw.githubusercontent.com/piyushpradhan22/credentials/refs/heads/main/credentials.json",
                   headers={"Authorization" : f"token {token}"}).json()

username = res['username']
password = res['password']

# Connection details
conn_info = {'host': 'localhost', 'port': 3333, 'username': username, 'password': password}

# Instantiate the client
qbt_client = qbittorrentapi.Client(**conn_info)
qbt_client.auth_log_in()

## Install Search Plugins
urls = ["https://gist.githubusercontent.com/scadams/56635407b8dfb8f5f7ede6873922ac8b/raw/f654c10468a0b9945bec9bf31e216993c9b7a961/one337x.py",
        "https://raw.githubusercontent.com/nindogo/qbtSearchScripts/master/torrentgalaxy.py",]
qbt_client.search_install_plugin(sources=urls)

print("Search Plugins installed")