from autopcr.model.requests import *
from autopcr.model.responses import *
from autopcr.core import pcrclient, ApiException, sessionmgr

def run_task():

    import asyncio

    accounts = []

    import os
    import json

    try:
        with open('dismissed.json', encoding='utf-8') as source:
            prev_dismissed = set(json.load(source))
    except (FileNotFoundError, json.JSONDecodeError):
        prev_dismissed = set()

    max_dismissed = max(prev_dismissed, default=0)
    last_not_dismissed = max(
        (x for x in range(max_dismissed) if x not in prev_dismissed), default=0
    )

    with open('farm_config.json', encoding='utf-8') as fp:
        j = json.load(fp)
    for acc in j['workers']:
        acc['account'] = acc['username']
        acc.pop('username')
        acc['platform'] = 2
        acc['channel'] = 1
        accounts.append(acc)

    if not accounts:
        return

    SEARCH_START = 0
    SEARCH_END = 2000000

    ids = [x for x in range(SEARCH_START, SEARCH_END) if x not in prev_dismissed or x >= last_not_dismissed]

    id_lck = asyncio.Lock()

    dismissed_t = []
    current = 0

    import datetime

    date = datetime.datetime.now().strftime('%Y-%m-%d')
    log_path = f'dbs/clan_{date}.log'
    fout_lck = asyncio.Lock()
    last_ok = -1

    async def worker(account):
        nonlocal current, last_ok
        client = pcrclient(account)
        
        for comp in client._components:
            if type(comp) is sessionmgr:
                comp: sessionmgr
                comp.auto_relogin = False

        await client.login()

        print(f'[{client.name}] logged in')

        while True:
            async with id_lck:
                if current >= len(ids):
                    break
                id = ids[current]
                current += 1
                if id > last_ok + 10000: # 1w个都没成功，认为已经无法成功
                    break
            while True:
                req = OtherClanInfoRequest()
                req.clan_id = id
                try:
                    resp = await client.request(req)
                    async with fout_lck:
                        last_ok = max(last_ok, id)
                        fout.write(f'{resp.model_dump_json()}\n')
                    print(f'[{id}] {resp.clan.detail.clan_name}')
                    break
                except ApiException as ex:
                    if '此行会已解散。' in ex.args[0]:
                        dismissed_t.append(id)
                        print(f'[{id}]{ex}')
                        break

                    print(f'[{id}]{ex}')
                    for comp in client._components:
                        if type(comp) is sessionmgr:
                            comp: sessionmgr
                            comp._logged = False
                    await client.login()

    async def run_workers():
        await asyncio.gather(*(worker(account) for account in accounts))

    os.makedirs('dbs', exist_ok=True)
    with open(log_path, 'w', encoding='utf-8') as fout:
        asyncio.run(run_workers())

    dismissed = prev_dismissed | {clan_id for clan_id in dismissed_t if clan_id < last_ok}
    with open('dismissed.json', 'w', encoding='utf-8') as target:
        json.dump(sorted(dismissed), target)

    os.system(f'ClanAnalyzer.exe {log_path}')
    os.remove(log_path)


import datetime
import time

if __name__ == '__main__':
    date = None
    while True:
        datet = datetime.datetime.now().strftime('%Y-%m-%d')
        if datet != date:
            run_task()
            date = datet
        else:
            time.sleep(60)
