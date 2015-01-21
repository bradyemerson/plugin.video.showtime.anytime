#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path
from datetime import date, datetime
from sqlite3 import dbapi2 as sqlite
from bs4 import BeautifulSoup

import simplejson as json

import xbmcvfs
import xbmcgui
import common
import connection
import database_common as db_common


def create():
    c = _database.cursor()
    c.execute('''CREATE TABLE series
                (series_id INTEGER PRIMARY KEY,
                 title TEXT,
                 title_sort TEXT,
                 plot TEXT,
                 directors TEXT,
                 actors TEXT,
                 thumb TEXT,
                 total_seasons INTEGER,
                 total_episodes INTEGER,
                 favor BOOLEAN DEFAULT 0,
                 in_last_update BOOLEAN DEFAULT 1,
                 last_updated timestamp);''')

    c.execute('''CREATE TABLE season
                (season_id INTEGER PRIMARY KEY,
                 season_no INTEGER,
                 series_id INTEGER,
                 plot TEXT,
                 FOREIGN KEY(series_id) REFERENCES series(series_id) ON DELETE CASCADE);''')

    c.execute('''CREATE TABLE episode
                (episode_id INTEGER PRIMARY KEY,
                 season_id INTEGER,
                 episode_no INTEGER,
                 title TEXT,
                 title_sort TEXT,
                 plot TEXT,
                 duration INTEGER,
                 year INTEGER,
                 studio TEXT,
                 mpaa TEXT,
                 advisories TEXT,
                 aired_date timestamp,
                 thumb TEXT,
                 play_count INTEGER DEFAULT 0,
                 FOREIGN KEY(season_id) REFERENCES season(season_id) ON DELETE CASCADE);''')

    _database.commit()
    c.close()


def insert_series(series_id, title=None, title_sort=None, plot=None, directors=None, actors=None, thumb=None,
                  total_seasons=None, total_episodes=None):
    c = _database.cursor()

    c.execute('''INSERT OR REPLACE INTO series (
                 series_id,
                 title,
                 title_sort,
                 plot,
                 directors,
                 actors,
                 thumb,
                 total_seasons,
                 total_episodes,
                 favor,
                 in_last_update,
                 last_updated) VALUES (
                 :series_id,
                 :title,
                 :title_sort,
                 :plot,
                 :directors,
                 :actors,
                 :thumb,
                 :total_seasons,
                 :total_episodes,
                 (SELECT favor FROM series WHERE series_id = :series_id),
                 :in_last_update,
                 (SELECT last_updated FROM series WHERE series_id = :series_id))''', {
        'series_id': series_id,
        'title': title,
        'title_sort': title_sort,
        'plot': plot,
        'directors': directors,
        'actors': actors,
        'thumb': thumb,
        'total_seasons': total_seasons,
        'total_episodes': total_episodes,
        'in_last_update': True
    })
    _database.commit()
    c.close()


def insert_season(series_id, season_no, plot=None):
    c = _database.cursor()
    row = lookup_season(series_id=series_id, season_no=season_no, fields='season_id').fetchone()
    if row:
        c.execute('''UPDATE season SET plot = :plot WHERE season_id = :season_id''', {
            'season_id': row['season_id'],
            'plot': plot
        })
    else:
        c.execute('''INSERT INTO season (series_id, season_no, plot) VALUES (
            :series_id,
            :season_no,
            :plot
          )''', {
            'series_id': series_id,
            'season_no': season_no,
            'plot': plot
        })

    _database.commit()
    c.close()


def insert_episode(episode_id, season_id, episode_no=None, title=None, title_sort=None, plot=None,
                   duration=None, year=None, studio=None, mpaa=None, advisories=None, aired_date=None, thumb=None):
    c = _database.cursor()

    c.execute('''INSERT OR REPLACE INTO episode (
                 episode_id,
                 season_id,
                 episode_no,
                 title,
                 title_sort,
                 plot,
                 duration,
                 year,
                 studio,
                 mpaa,
                 advisories,
                 aired_date,
                 thumb,
                 play_count) VALUES (
                 :episode_id,
                 :season_id,
                 :episode_no,
                 :title,
                 :title_sort,
                 :plot,
                 :duration,
                 :year,
                 :studio,
                 :mpaa,
                 :advisories,
                 :aired_date,
                 :thumb,
                 COALESCE((SELECT play_count FROM episode WHERE episode_id = :episode_id), 0))''', {
        'episode_id': episode_id,
        'season_id': season_id,
        'episode_no': episode_no,
        'title': title,
        'title_sort': title_sort,
        'plot': plot,
        'duration': duration,
        'year': year,
        'studio': studio,
        'mpaa': mpaa,
        'advisories': advisories,
        'aired_date': aired_date,
        'thumb': thumb
    })
    _database.commit()
    c.close()


def lookup_series(content_id, fields='*'):
    c = _database.cursor()
    return c.execute('SELECT DISTINCT {0} FROM series WHERE series_id = (?)'.format(fields), (content_id,))


def lookup_season(season_id=None, series_id=None, season_no=None, fields='*'):
    c = _database.cursor()
    if season_id:
        return c.execute('SELECT {0} FROM season WHERE season_id = (?)'.format(fields), (season_id,))
    elif series_id and season_no:
        return c.execute('SELECT {0} FROM season WHERE series_id = (?) AND season_no = (?)'.format(fields),
                         (series_id, season_no))


def lookup_episode(content_id):
    c = _database.cursor()
    return c.execute('SELECT DISTINCT * FROM episode WHERE episode_id = (?)', (content_id,))


def delete_series(content_id):
    c = _database.cursor()
    c.execute('DELETE FROM series WHERE series_id = (?)', (content_id,))
    c.close()


def watch_episode(content_id):
    # TODO make this actually increment
    c = _database.cursor()
    c.execute("UPDATE episode SET play_count = play_count + 1 WHERE episode_id = (?)", (content_id,))
    _database.commit()
    c.close()
    return c.rowcount


def unwatch_episode(content_id):
    c = _database.cursor()
    c.execute("UPDATE episode SET play_count=? WHERE episode_id = (?)", (0, content_id))
    _database.commit()
    c.close()
    return c.rowcount


def favor_series(content_id):
    c = _database.cursor()
    c.execute("UPDATE series SET favor=? WHERE series_id=?", (True, content_id))
    _database.commit()
    c.close()
    return c.rowcount


def unfavor_series(content_id):
    c = _database.cursor()
    c.execute("UPDATE series SET favor=? WHERE series_id=?", (False, content_id))
    _database.commit()
    c.close()
    return c.rowcount


def get_series(directorfilter=False, watchedfilter=False, favorfilter=False, actorfilter=False,
               alphafilter=False, studiofilter=False):
    c = _database.cursor()
    if actorfilter:
        actorfilter = '%' + actorfilter + '%'
        return c.execute('SELECT DISTINCT * FROM series WHERE actors LIKE (?)',
                         (actorfilter,))
    elif directorfilter:
        return c.execute('SELECT DISTINCT * FROM series WHERE directors LIKE (?)',
                         (directorfilter,))
    elif studiofilter:
        return c.execute('SELECT DISTINCT * FROM series WHERE studio = (?)', (studiofilter,))
    elif watchedfilter:
        return c.execute('SELECT DISTINCT * FROM series WHERE playcount > 0')
    elif favorfilter:
        return c.execute('SELECT DISTINCT * FROM series WHERE favor = 1')
    elif alphafilter:
        return c.execute('SELECT DISTINCT * FROM series WHERE title REGEXP (?)',
                         (alphafilter + '*',))
    else:
        return c.execute('SELECT DISTINCT * FROM series')


def get_series_season_count(series_id):
    c = _database.cursor()
    row = c.execute('''SELECT MAX(sea.content_id) AS total_seasons
          FROM season AS sea
          JOIN series AS ser ON ser.content_id = sea.series_content_id
          WHERE ser.content_id = (?)
          GROUP BY ser.content_id''', (series_id,)).fetchone()
    c.close()
    if row:
        return row['total_seasons']
    else:
        return 0


def get_series_episode_count(series_id, filter=None):
    c = _database.cursor()
    if filter == 'watched':
        row = c.execute('''SELECT COUNT(e.episode_id) AS total_episodes
              FROM episode AS e
              JOIN season AS sea ON sea.season_id = e.season_id
              JOIN series AS ser ON ser.series_id = sea.series_id
              WHERE ser.series_id = (?) AND e.play_count > 0
              GROUP BY ser.series_id''', (series_id,)).fetchone()
    else:
        row = c.execute('''SELECT COUNT(e.episode_id) AS total_episodes
              FROM episode AS e
              JOIN season AS sea ON sea.season_id = e.season_id
              JOIN series AS ser ON ser.series_id = sea.series_id
              WHERE ser.series_id = (?)
              GROUP BY ser.series_id''', (series_id,)).fetchone()
    c.close()
    if row:
        return row['total_episodes']
    else:
        return 0


def get_series_year(series_id):
    c = _database.cursor()
    row = c.execute('''SELECT e.year FROM episode AS e
                  JOIN season AS sea ON sea.season_id = e.season_id
                  JOIN series AS ser ON ser.series_id = sea.series_id
                  WHERE ser.series_id = (?)
                  ORDER BY e.year ASC LIMIT 1''', (series_id,)).fetchone()
    c.close()
    if row:
        return row['year']
    else:
        return None


def _update_series_last_update(series_id, time=datetime.now()):
    c = _database.cursor()
    c.execute('UPDATE series SET last_updated = :last_update WHERE series_id = :series_id', {
        'last_update': time,
        'series_id': series_id
    })
    c.close()


def get_seasons(series_id):
    c = _database.cursor()
    return c.execute('''SELECT DISTINCT sea.*,ser.title AS series_title
                        FROM season AS sea
                        JOIN series AS ser ON ser.series_id = sea.series_id
                        WHERE ser.series_id = (?)''', (series_id,))


def get_season_episode_count(season_id, filter=None):
    c = _database.cursor()
    if filter == 'watched':
        row = c.execute('''SELECT COUNT(e.episode_id) AS total_episodes
            FROM episode AS e
            JOIN season AS sea ON sea.season_id = e.season_id
            WHERE sea.season_id = (?) AND e.play_count > 0
            GROUP BY sea.season_id''', (season_id,)).fetchone()
    else:
        row = c.execute('''SELECT COUNT(e.episode_id) AS total_episodes
            FROM episode AS e
            JOIN season AS sea ON sea.season_id = e.season_id
            WHERE sea.season_id = (?)
            GROUP BY sea.season_id''', (season_id,)).fetchone()
    c.close()
    if row:
        return row['total_episodes']
    else:
        return 0


def get_season_year(season_id):
    c = _database.cursor()
    row = c.execute('''SELECT e.year FROM episode AS e
        JOIN season AS sea ON sea.season_id = e.season_id
        WHERE sea.season_id = (?)
        ORDER BY e.year ASC LIMIT 1''', (season_id,)).fetchone()
    c.close()
    if row:
        return row['year']
    else:
        return None


def get_episodes(season_id):
    c = _database.cursor()
    return c.execute('''SELECT DISTINCT e.*, sea.season_no AS season_no, ser.title AS series_title, ser.series_id AS series_id
        FROM episode AS e
        JOIN season AS sea ON sea.season_id = e.season_id
        JOIN series AS ser ON ser.series_id = sea.series_id
        WHERE e.season_id = (?)''', (season_id,))


def get_types(col):
    c = _database.cursor()
    items = c.execute('select distinct %s from series' % col)
    list = []
    for data in items:
        data = data[0]
        if type(data) == type(str()):
            if 'Rated' in data:
                item = data.split('for')[0]
                if item not in list and item <> '' and item <> 0 and item <> 'Inc.' and item <> 'LLC.':
                    list.append(item)
            else:
                data = data.decode('utf-8').encode('utf-8').split(',')
                for item in data:
                    item = item.replace('& ', '').strip()
                    if item not in list and item <> '' and item <> 0 and item <> 'Inc.' and item <> 'LLC.':
                        list.append(item)
        elif data <> 0:
            if data is not None:
                list.append(str(data))
    c.close()
    return list


def update_tv(force=False):
    # Check if we've recently updated and skip
    if not force and not _needs_update():
        return

    dialog = xbmcgui.DialogProgress()
    dialog.create('Refreshing TV Database')
    dialog.update(0, 'Initializing TV Scan')

    xml_series_url = '{0}/tve/xml/category?categoryid=101'.format(db_common.API_DOMAIN)
    data = connection.get_url(xml_series_url)
    soup = BeautifulSoup(data)
    series_list = soup.find('subcategory', recursive=False).find('series', recursive=False).find_all('series', recursive=False)

    # Mark all series as unfound. This will be updated as we go through
    c = _database.cursor()
    c.execute("UPDATE series SET in_last_update = 0")
    _database.commit()
    c.close()

    total = len(series_list)
    count = 0

    for series in series_list:
        count += 1
        dialog.update(0, 'Scanned {0} of {1} TV series'.format(count, total))

        print 'series: '
        print series

        series_json_url = '{0}/api/series/{1}'.format(db_common.API_DOMAIN, series['seriesid'])
        json_data = json.loads(connection.get_url(series_json_url))

        series_id = series['seriesid']
        title = common.string_unicode(json_data['name'])
        title_sort = common.string_unicode(json_data['sortName'])
        plot = common.string_unicode(json_data['description']['long'])
        total_seasons = json_data['totalSeasons']
        total_episodes = json_data['totalEpisodes']

        thumb = None
        for image in series.find_all('Image'):
            if image['width'] == '1920' and image['height'] == '1080':
                thumb = image.find('url').string
                break

        insert_series(series_id, title, title_sort, plot, None, None, thumb, total_seasons, total_episodes)

        # Season Children
        if 'seasons' in json_data:
            _json_process_seasons(json_data['seasons'], series_id)

    _set_last_update()

    # Remove unfound movies
    c = _database.cursor()
    c.execute("DELETE FROM series WHERE in_last_update = 0")
    c.close()


def _json_process_seasons(season_data, series_id):
    for season in season_data:
        insert_season(series_id, season['seasonNum'], season['description']['long'])


def update_series(series_id, force=False):
    # Check for new episodes every 12 hours
    row = lookup_series(series_id, 'last_updated').fetchone()
    if force is False and row['last_updated']:
        last_update = common.parse_date(row['last_updated'], '%Y-%m-%d %H:%M:%S.%f')
        if (datetime.now() - last_update).seconds < 43200:
            # No update needed
            return

    xml_series_url = '{0}/tve/xml/series?seriesid={1}'.format(db_common.API_DOMAIN, series_id)
    data = connection.get_url(xml_series_url)
    series = BeautifulSoup(data).find('series', recursive=False)

    for episode in series.find_all('title', attrs={'type': 'Episode'}):
        episode_id = episode['titleid']
        title = common.string_unicode(episode.find('title', recursive=False).string)
        title_sort = common.string_unicode(episode.find('sorttitle', recursive=False).string)
        plot = common.string_unicode(episode.find('description', recursive=False).string)
        year = episode.find('releaseyear', recursive=False).string
        duration = episode.find('duration', recursive=False).string
        mpaa = episode.find('rating', recursive=False).string
        advisories = episode.find('advisories', recursive=False).string

        air_date = None
        try:
            air_date = common.parse_date(episode.find('originalairdate', recursive=False).string, '%m/%d/%Y %I:%M%p')
        except:
            pass

        thumb = None
        for image in episode.find_all('image'):
            if image['width'] == '866' and image['height'] == '487':
                thumb = image.find('url').string
                break

        series_tag = episode.find('series', recursive=False)
        episode_no = series_tag['episode']
        season_no = series_tag['season']
        season = lookup_season(series_id=series_id, season_no=season_no, fields='season_id').fetchone()
        if not season:
            insert_season(series_tag['seriesid'], season_no)
            season = lookup_season(series_id=series_id, season_no=season_no, fields='season_id').fetchone()

        season_id = season['season_id']

        insert_episode(episode_id, season_id, episode_no, title, title_sort, plot, duration, year, None,
                       mpaa, advisories, air_date, thumb)

    _update_series_last_update(series_id)


def _needs_update():
    # Update every 15 days
    if 'last_update' in _database_meta:
        last_update = common.parse_date(_database_meta['last_update'], '%Y-%m-%d')
        return (date.today() - last_update.date()).days > 15

    return True


def _set_last_update():
    _database_meta['last_update'] = date.today().strftime('%Y-%m-%d')
    _write_meta_file()


def _write_meta_file():
    f = open(DB_META_FILE, 'w')
    json.dump(_database_meta, f)
    f.close()


DB_META_FILE = os.path.join(common.__addonprofile__, 'tv.meta')
_database_meta = False
if xbmcvfs.exists(DB_META_FILE):
    f = open(DB_META_FILE, 'r')
    _database_meta = json.load(f)
    f.close()
else:
    _database_meta = {}

DB_FILE = os.path.join(common.__addonprofile__, 'tv.db')
if not xbmcvfs.exists(DB_FILE):
    _database = sqlite.connect(DB_FILE)
    _database.text_factory = str
    _database.row_factory = sqlite.Row
    create()
else:
    _database = sqlite.connect(DB_FILE)
    _database.text_factory = str
    _database.row_factory = sqlite.Row
