import cx_Oracle
import pandas as pd
import requests
import random
from random import sample
from tqdm import tqdm
tqdm.pandas()

dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
api_key='RGAPI-c0860d43-8d30-4256-8665-1d6d163007ae'
api_key2='6c6a755169736a6a3833456d626662'


def df_creater(url):
    url=url.replace('(인증키)',api_key2).replace('xml','json').replace('/5','/1000').replace('/20130615','/20220615')
    res=requests.get(url).json()
    key=list(res.keys())[0]
    data=res[key]['row']
    df=pd.DataFrame(data)
    return df

def db_open():
    global db
    global cursor
    try:
        cx_Oracle.init_oracle_client(lib_dir=r"C:\oraclexe\app\oracle\instantclient_21_6")
        db = cx_Oracle.connect(user='ADMIN', password='3460718aAAAA', dsn='db20220623152020_high')
        cursor = db.cursor()
        print('open!')
    except:
        db = cx_Oracle.connect(user='ADMIN', password='3460718aAAAA', dsn='db20220623152020_high')
        cursor = db.cursor()
        print('open!')

def db_open_local():
    global db
    global cursor
    db = cx_Oracle.connect(user='icia', password='1111', dsn=dsn)
    cursor = db.cursor()
    print('open!')


def sql_execute(q):
    global db
    global cursor
    try:
        if 'select' in q:
            df = pd.read_sql(sql=q, con=db)
            return df
        cursor.execute(q)
        return 'success!'
    except Exception as e:
        print(e)

def db_close():
    global db
    global cursor
    try:
        db.commit()
        cursor.close()
        db.close()
        return 'close!'
    except Exception as e:
        print(e)
        
def get_puuid(name):
    url='https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/'+name+'?api_key='+api_key
    res=requests.get(url).json()
    puuid=res['puuid']
    return puuid

def get_match_Id(puuid,num):
    url='https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/'+puuid+'/ids?start=0&count='+str(num)+'&api_key='+api_key
    res=requests.get(url).json()
    return res

def get_matches_timelines(matchids):
    lst=[]
    for match_id in tqdm(matchids):
        url='https://asia.api.riotgames.com/lol/match/v5/matches/'+match_id+'?api_key='+api_key
        res1=requests.get(url).json()
        url='https://asia.api.riotgames.com/lol/match/v5/matches/'+match_id+'/timeline?api_key='+api_key
        res2=requests.get(url).json()
        lst.append([match_id,res1,res2])
    return lst


def get_rowData(tier):
    division_lst=['I', 'II', 'III', 'IV']
    lst=[]
    page=random.randrange(1,10)
    
    for division in tqdm(division_lst):
        url='https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/'+tier+'/'+division+'?page='+str(page)+'&api_key='+api_key
        res=requests.get(url).json()
        lst+=sample(res,3)
    
    print('get summonerName....')
    summonerName_lst=list(map(lambda x:x['summonerName'],lst))
    
    print('get puuid....')
    puuid_lst=[]
    for n in tqdm(summonerName_lst):
        try:
            puuid_lst.append(get_puuid(n))
        except:
            print(n)
            continue
            
    print('get_match_id....')
    matchid_lst=[]
    for p in tqdm(puuid_lst):
        matchid_lst.extend(get_match_Id(p,2))
        
    match_timeline_lst=get_match(matchid_lst)
        
    df=pd.DataFrame(match_timeline_lst,columns=['gameid', 'matches', 'timeline'])
    print('complete!')
    return df

def get_match_timeline_df(df):
    # matchdf,timelinedf 두개를 만들기 or 두개를 하나로 만들기
    df_creater=[]
    print('소환사 스텟 생성중')
    for i in tqdm(range(len(df))):
        try:
            if(df.iloc[i].matches['info']['gameMode']=='CLASSIC'):
                for j in range(10):
                    tmp=[]
                    tmp.append(df.iloc[i].gameid)
                    tmp.append(df.iloc[i].matches['info']['gameDuration'])
                    tmp.append(df.iloc[i].matches['info']['gameVersion'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])
                    for k in range(5,26):
                        try:
                            tmp.append(df.iloc[i].timeline['info']['frames'][k]['participantFrames'][str(j+1)]['totalGold'])
                        except:
                            tmp.append(0)
                    df_creater.append(tmp)
        except:
            continue
            
    columns = ['gameId','gameDuration','gameVersion','summonerName','summonerLevel','participantId','championName','champExperience',
           'teamPosition','teamId','win','kills','deaths','assists','totalDamageDealtToChampions','totalDamageTaken','g_5','g_6','g_7','g_8','g_9','g_10','g_11','g_12','g_13','g_14','g_15','g_16','g_17',
           'g_18','g_19','g_20','g_21','g_22','g_23','g_24','g_25']
    df=pd.DataFrame(df_creater,columns=columns).drop_duplicates()
    print('complete! 현재 df의 수는 %d 입니다.'% len(df))
    return df
   

def insert_matches_timeline(row):
    #lambda를 이용해서 progress_apply를 통해 insert할 구문 만글기
    query=(f'merge into match_stats using dual on(gameId=\'{row.gameId}\' and participantId={row.participantId}) '
           f'when not matched then '
           f'insert(gameId,gameDuration,gameVersion,summonerName,summonerLevel,participantId,championName,champExperience,teamPosition,'
           f'teamId,win,kills,deaths,assists,totalDamageDealtToChampions,totalDamageTaken,g_5,g_6,g_7,g_8,g_9,g_10,g_11,g_12,g_13,g_14,g_15,'
           f'g_16,g_17,g_18,g_19,g_20,g_21,g_22,g_23,g_24,g_25)'
           f'values(\'{row.gameId}\',\'{row.gameDuration}\',\'{row.gameVersion}\',\'{row.summonerName}\',\'{row.summonerLevel}\','
          f'\'{row.participantId}\',\'{row.championName}\',\'{row.champExperience}\',\'{row.teamPosition}\',{row.teamId},\'{row.win}\',{row.kills},{row.deaths},'
          f'{row.assists},{row.totalDamageDealtToChampions},{row.totalDamageTaken},{row.g_5},{row.g_6},{row.g_7},{row.g_8},{row.g_9},{row.g_10},{row.g_11},'
           f'{row.g_12},{row.g_13},{row.g_14},{row.g_15},{row.g_16},{row.g_17},{row.g_18},{row.g_19},{row.g_20},{row.g_21},{row.g_22},{row.g_23},{row.g_24},{row.g_25})'
          )
    sql_execute(query)
    return query

def data_insert(row):
    query = (
        f'merge into rank_data using dual on(gameId=\'{row.gameId}\' and participantId={row.participantId}) '
        f'when not matched then '
        f'insert (gameId, gameDuration, gameVersion, summonerName, participantId, championName, '
        f'lane, teamId, win, kills, deaths, assists, damageDealt, damageTaken, bans,'
        f'gold_5_35, killerId, victimId, assistId, '
        f'firstLaneTower, laneTower, timeLaneTower, g15, lv6_time) '
        f'values(\'{row.gameId}\', {row.gameDuration}, '
        f'\'{row.gameVersion}\', \'{row.summonerName}\', {row.participantId}, '
        f'\'{row.championName}\', \'{row.lane}\', {row.teamId}, \'{row.win}\', '
        f'{row.kills}, {row.deaths}, {row.assists}, {row.damageDealt}, {row.damageTaken}, \'{row.bans}\', '
        f'\'{row.gold_5_35}\', \'{row.killerId}\', \'{row.victimId}\', \'{row.assistId}\', '
        f'{row.firstLaneTower}, {row.laneTower}, {row.timeLaneTower}, {row.g15}, {row.lv6_time} ) '
    )
    mu.sql_execute(query)
    return