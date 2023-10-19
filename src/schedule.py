import os, yaml
from bs4 import BeautifulSoup
from datetime import datetime

fp = os.path.dirname(os.path.realpath(__file__))
with open(fp + '/schedule.yaml', 'r') as f:
    schedule = yaml.load(f)

def str_to_dt(string):
    dt_obj = datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
    return(dt_obj)

def check_dt(dt, start_dt, end_dt):
    flag = start_dt <= dt <= end_dt
    return(flag)

def get_week_urls():
    dt = datetime.today()
    for week_id, data in schedule.items():
        start_dt = str_to_dt(data['start'])
        end_dt = str_to_dt(data['end'])
        if check_dt(dt, start_dt, end_dt):
            return(week_id, data)
            break
