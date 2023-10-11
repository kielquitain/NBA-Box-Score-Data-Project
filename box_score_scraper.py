"""NBA SCRAPER"""

import pandas as pd
from bs4 import BeautifulSoup
import requests
from random import randint
import lxml
import time
import logging
import gspread
from gspread_dataframe import set_with_dataframe
import gspread_dataframe as gd


TEAM1_INDEX = 0
TEAM2_INDEX = 8
URL = f'https://www.basketball-reference.com/boxscores/?month={3}&day={10}&year={2023}'
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1_6GndTYwsc3Xol99RK4HYc8A0x76tbTSUPj-117Uum4/edit#gid=0'


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
    team1_name = title.split(', ')[0].split('vs')[0].strip()
    # Team 1
    team1_table = pd.read_html(box_score_url, header=1)[TEAM1_INDEX]
    team1_table.rename(columns={'Starters': 'Players'}, inplace=True)
    team1_df = team1_table.drop(team1_table[team1_table['Players'] == 'Reserves'].index)
    team1_df.rename({'+/-': 'Plus/Minus'}, inplace=True)
    team1_df['Team'] = team1_name
    # Team 2
    team2_table = pd.read_html(box_score_url, header=1)[TEAM2_INDEX]
    team2_name = title.split(', ')[0].split('vs')[0].strip()
    team2_table.rename(columns={'Starters': 'Players'}, inplace=True)
    team2_df = team2_table.drop(team1_table[team1_table['Players'] == 'Reserves'].index)
    team2_df.rename({'+/-': 'Plus/Minus'}, inplace=True)
    team2_df['Team'] = team2_name
    
    combined_df = pd.concat([team1_df, team2_df])
    print(combined_df)
    return combined_df


def soupify(url):
    res = requests.get(url)
    # with open('team1.html', 'w', encoding="utf-8") as html_file: 
    #     html_file.write(res.text)
    #     html_file.close()
    soup = BeautifulSoup(res.text, 'html.parser')
    return soup

def main():
    """Functions here"""
    gc = gspread.service_account('service_account.json')
    ws = gc.open("NBA Box Score Database").worksheet("DB")


    box_score_urls = get_box_scores(daily_url=URL)
    all_dfs = []
    for url in box_score_urls[2:3]:
        print('URL: ', url)
        all_dfs.append(
            scrape_box_score(box_score_url=url)
        )
        time.sleep(randint(10, 15))

    final_df = pd.concat(all_dfs)
    existing_df = gd.get_as_dataframe(worksheet=ws, usecols=['Players', 'MP', 'FG', 'FGA',
                                            'FG%','3P','3PA','3P%','FT',
                                            'FTA','FT%','ORB','DRB','TRB',
                                            'AST','STL','BLK','TOV','PF',
                                            'PTS','Plus/Minus','Team'])
    updated_df = pd.concat([existing_df, final_df])
    updated_df = updated_df.dropna()
    print(updated_df)
    gd.set_with_dataframe(ws, updated_df)

    # set_with_dataframe(worksheet=ws, dataframe=df, include_index=False,
    #                     include_column_header=False, resize=True)
if __name__ == '__main__':
    main()