"""NBA SCRAPER"""
import pandas as pd
from bs4 import BeautifulSoup
import requests
import io
from random import randint
import time
import gspread
import gspread_dataframe as gd
from datetime import datetime
import re
import sys


def get_box_scores(daily_url):
    """Get all of the box scores daily"""
    soup = soupify(daily_url)
    box_scores_list = []
    box_scores = soup.find_all(lambda tag:tag.name=="a" and "Box Score" == tag.text)

    for box_score in box_scores:
        box_scores_list.append(
            'https://www.basketball-reference.com/' + box_score['href']
        )
    return box_scores_list

def scrape_box_score(box_score_url):
    """Scrape the box scores"""
    soup = soupify(box_score_url)
    title = soup.find('title').text
    tables = soup.find_all('table', {'id': re.compile(r'box-([A-Z]{3})-game-basic')})
    tables_str = io.StringIO(str(tables))
    tables = pd.read_html(tables_str, header=1, flavor='bs4')

    # Team 1
    team1_table = tables[0]
    team1_name = title.split(', ')[0].split('vs')[0].strip()
    team1_table.rename(columns={'Starters': 'Players'}, inplace=True)
    team1_df = team1_table.drop(team1_table[team1_table['MP'] == 'MP'].index)
    team1_df = team1_df.drop(team1_df[team1_df['Players'] == 'Team Totals'].index)
    team1_df.columns.values[20] = "Plus/Minus"
    team1_df['Team'] = team1_name
    team1_df['Game Date'] = datetime.now().strftime('%Y-%m-%d')

    # Team 2
    team2_table = tables[1]
    team2_name = title.split(', ')[0].split('vs')[1].strip()
    team2_table.rename(columns={'Starters': 'Players'}, inplace=True)
    team2_df = team2_table.drop(team2_table[team2_table['MP'] == 'MP'].index)
    team2_df = team2_df.drop(team2_df[team2_df['Players'] == 'Team Totals'].index)
    team2_df.columns.values[20] = "Plus/Minus"
    team2_df['Team'] = team2_name
    team2_df['Game Date'] = datetime.now().strftime('%Y-%m-%d')
    combined_df = pd.concat([team1_df, team2_df])
    return combined_df


def soupify(url):
    "Get soup object from html"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    return soup

def main():
    """Functions here"""
    day = datetime.today().day - datetime.timedelta(days = 1)
    month = datetime.today().month
    year = datetime.today().year
    base_url = f'https://www.basketball-reference.com/boxscores/?month={month}&day={day}&year={year}'
    print(base_url)
    
    gc = gspread.service_account('service_account.json')
    ws = gc.open("NBA Box Score Database").worksheet("DB")
    
    box_score_urls = get_box_scores(daily_url=base_url)

    if len(box_score_urls) < 1:
        print('No Box Score Available')
        sys.exit()

    all_dfs = []

    for url in box_score_urls:
        all_dfs.append(
            scrape_box_score(box_score_url=url)
        )
        time.sleep(randint(10, 15))
        print(f'Scrape Done with URL: {url}')
    
    final_df = pd.concat(all_dfs)
    gd.set_with_dataframe(ws, final_df)
    print('Sheets Updated!')


if __name__ == '__main__':
    main()