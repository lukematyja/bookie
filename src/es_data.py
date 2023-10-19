import json, time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import boto3
import logging as log
log.basicConfig(format='ES | %(asctime)s | %(levelname)s | %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S %p',
                level=log.INFO)

class Game():
    def __init__(self, game_soup, status_flag):
        self.game_soup = game_soup
        self.status_flag = status_flag
        keys = [
            'game_status',
            'game_ts',
            'game_id',
            'away_team_id',
            'home_team_id',
            'underdog_id',
            'spread',
            'away_score',
            'home_score',
            'winner'
        ]
        self.data = dict.fromkeys(keys)
        self.main()

    def game(self):
        game_id = {
            'game_status' : self.status_flag,
            'game_id'     : self.game_soup.attrs['id']
        }
        if self.status_flag == 'PREGAME':
            date_raw = self.game_soup.find('th', {'class': 'date-time'})['data-date']
            date_obj_utc = datetime.strptime(date_raw, "%Y-%m-%dT%H:%MZ")
            date_obj_etc = date_obj_utc - timedelta(hours=4)
            game_id.update({'game_ts' : date_obj_etc.strftime('%Y-%m-%d %H:%M')})
        self.data.update(game_id)
        short_names = self.game_soup.findAll('span', {'class': 'sb-team-abbrev'})
        self.short_names = [elem.contents[0] for elem in short_names]
        self.short_names_str = ' at '.join(self.short_names)
        return(self.short_names_str)

    def teams(self):
        teams_raw = [
            self.game_soup.attrs['data-awayid'],
            self.game_soup.attrs['data-homeid']
        ]
        team_keys = ['away_team_id', 'home_team_id']
        self.team_ids = {key: team_raw for key, team_raw in zip(team_keys, teams_raw)}
        self.data.update(self.team_ids)
        return(self.team_ids)

    def spread(self):
        try:
            spread_raw = self.game_soup.find('th', {'class': 'line'}).contents[0]
            spread_split = str.split(spread_raw, ' ')
            favorite = spread_split[0]
            favorite_id = self.short_names.index(favorite)
            underdog_idx = abs(favorite_id - 1)
            spreads = {
                'underdog_id' : list(self.team_ids.values())[underdog_idx],
                'spread'   : abs(float(spread_split[1]))
            }
            self.data.update(spreads)
        except AttributeError:
            log.warning('No spread available')
            pass

    def score(self):
        scores_raw = self.game_soup.findAll('td', {'class' : 'total'})
        if self.status_flag == 'AWAY_WIN':
            winner = self.data['away_team_id']
        elif self.status_flag == 'HOME_WIN':
            winner = self.data['home_team_id']
        else:
            winner = None
        score = {
            'away_score' : int(scores_raw[0].find('span').contents[0]),
            'home_score' : int(scores_raw[1].find('span').contents[0]),
            'winner'     : winner
        }
        self.data.update(score)

    def main(self):
        self.game()
        self.teams()
        if self.status_flag == 'PREGAME':
            self.spread()
        else:
            self.score()

class ProcessGamesToS3():
    def __init__(self, soup, week_id):
        self.soup = soup
        self.week_id = week_id
        self.pregames_soup = self.soup.findAll("article",
                {"class": "scoreboard football pregame js-show"})
        self.inprogress_soup = self.soup.findAll("article",
                {"class": "scoreboard football live js-show"})
        self.away_winners_soup = self.soup.findAll('article',
                {'class': 'scoreboard football final away-winner js-show'})
        self.home_winners_soup = self.soup.findAll('article',
                {'class': 'scoreboard football final home-winner js-show'})
        self.main()

    def write_s3(self, dictionary, filepath):
        s3 = boto3.resource('s3')
        s3object = s3.Object('atthletics', filepath)
        s3object.put(
            Body = (bytes(json.dumps(dictionary, indent=4).encode('UTF-8')))
        )

    def process(self, games_soup, status_flag):
        games = []
        for game_soup in games_soup:
            game_obj = Game(game_soup, status_flag)
            log.info(status_flag + ' Processing: ' + game_obj.short_names_str)
            games.append(game_obj.data)
        return(games)

    def main(self):
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")
        pregames = self.process(self.pregames_soup, 'PREGAME')
        inprogress = self.process(self.inprogress_soup, 'INPROGRESS')
        away_winners = self.process(self.away_winners_soup, 'AWAY_WIN')
        home_winners = self.process(self.home_winners_soup, 'HOME_WIN')
        self.games = pregames + inprogress + away_winners + home_winners
        fp_params = {'week_id' : self.week_id, 'ts' : ts}
        s3_fp = 'data/es/week_id={week_id}/{ts}.json'.format(**fp_params)
        self.write_s3(self.games, s3_fp)
