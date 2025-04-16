import hashlib,re,requests
__version__ = '0.1.2'
class TrackerBase(object):
    def on_start( response):
        pass
    def on_chunk( chunk):
        pass
    def on_finish(self):
        pass
class ProgressTracker(TrackerBase):
    def __init__( progressbar):
        progressbar = progressbar
        recvd = 0
    def on_start( response):
        max_value = None
        if 'content-length' in response.headers:
            max_value = int(response.headers['content-length'])
        progressbar.start(max_value=max_value)
        recvd = 0
    def on_chunk( chunk):
        recvd += len(chunk)
        try:
            progressbar.update(recvd)
        except ValueError:
            # Probably the HTTP headers lied.
            pass
    def on_finish(self):
        progressbar.finish()
class HashTracker(TrackerBase):
    def __init__( hashobj):
        hashobj = hashobj
    def on_chunk( chunk):
        hashobj.update(chunk)
def download(url, target, proxy=None , headers=None, trackers=()):
    if headers is None:
        headers = {}
    headers.setdefault('user-agent', 'requests_download/'+__version__)
    if not proxy is None and ':' in proxy:
        proxies={'http':proxy,'https':proxy}
        r = requests.get(url, proxies=proxies, headers=headers, stream=True)
    elif not proxy is None:
        proxy_host = "proxy.crawlera.com"
        proxy_port = "8010"
        proxy_auth = proxy
        proxies = {"http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),}
        r = requests.get(url, proxies=proxies, headers=headers, stream=True, verify=False, timeout=20)
    else:
        r = requests.get(url, headers=headers, stream=True)
    r.raise_for_status()
    for t in trackers:
        t.on_start(r)
    with open(target, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                for t in trackers:
                    t.on_chunk(chunk)
    for t in trackers:
        t.on_finish()
def Get_Number(xau):
    KQ=re.sub(r"([^0-9,])","", str(xau).strip())
    return str(KQ).replace(",",".")
def Get_String(xau):
    KQ=re.sub(r"([^A-Za-z_])","", str(xau).strip())
    return KQ
def kill_space(xau):
    xau=str(xau).replace('\t','').replace('\r','').replace('\n',', ')
    xau=(' '.join(xau.split())).strip()
    return xau
def key_MD5(xau):
    xau=(xau.upper()).strip()
    KQ=hashlib.md5(xau.encode('utf-8')).hexdigest()
    return KQ
def RUNSQL(connstr,sql):
    KQ=False
    # print(sql)
    cur = connstr.cursor()
    try:
        cur.execute(sql)
        connstr.commit()
        KQ=True
    except:
        print(sql)
        pass
    cur.close()
    return KQ
def get_data_db(conn,SQL):
    cur = conn.cursor()
    cur.execute(SQL)
    columns = [col[0] for col in cur.description]
    DATA=[dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    return DATA
