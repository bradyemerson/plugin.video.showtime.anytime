#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path
import re
from datetime import date, datetime
import time
from sqlite3 import dbapi2 as sqlite

import simplejson as json

import xbmcgui
import common
import connection
import database_common as db_common
from bs4 import BeautifulSoup


def create():
    c = _database.cursor()
    c.execute('''CREATE TABLE movies
                (movie_id INTEGER PRIMARY KEY,
                 title TEXT,
                 title_sort TEXT,
                 plot TEXT,
                 duration INTEGER,
                 year INTEGER,
                 studio TEXT,
                 mpaa TEXT,
                 advisories TEXT,
                 directors TEXT,
                 writers TEXT,
                 actors TEXT,
                 actors_and_roles TEXT,
                 genres TEXT,
                 poster TEXT,
                 thumb TEXT,
                 play_count INTEGER DEFAULT 0,
                 favor BOOLEAN DEFAULT 0,
                 in_last_update BOOLEAN DEFAULT 1)''')
    _database.commit()
    c.close()


def insert(movie_id, title=None, title_sort=None, plot=None, duration=None, year=None, studio=None, mpaa=None,
           advisories=None, directors=None, writers=None, actors=None, actors_and_roles=None, genres=None, poster=None,
           thumb=None):
    c = _database.cursor()

    c.execute('''INSERT OR REPLACE INTO movies (
                 movie_id,
                 title,
                 title_sort,
                 plot,
                 duration,
                 year,
                 studio,
                 mpaa,
                 advisories,
                 directors,
                 writers,
                 actors,
                 actors_and_roles,
                 genres,
                 poster,
                 thumb,
                 play_count,
                 favor,
                 in_last_update) VALUES (
                 :movie_id,
                 :title,
                 :title_sort,
                 :plot,
                 :duration,
                 :year,
                 :studio,
                 :mpaa,
                 :advisories,
                 :directors,
                 :writers,
                 :actors,
                 :actors_and_roles,
                 :genres,
                 :poster,
                 :thumb,
                 COALESCE((SELECT play_count FROM movies WHERE movie_id = :movie_id), 0),
                 (SELECT favor FROM movies WHERE movie_id = :movie_id),
                 :in_last_update)''', {
        'movie_id': movie_id,
        'title': title,
        'title_sort': title_sort,
        'plot': plot,
        'duration': duration,
        'year': year,
        'studio': studio,
        'mpaa': mpaa,
        'advisories': advisories,
        'directors': directors,
        'writers': writers,
        'actors': actors,
        'actors_and_roles': actors_and_roles,
        'genres': genres,
        'poster': poster,
        'thumb': thumb,
        'in_last_update': True
    })
    _database.commit()
    c.close()


def get_movie(movie_id):
    c = _database.cursor()
    return c.execute('SELECT DISTINCT * FROM movies WHERE movie_id = (?)', (movie_id,))


def delete(movie_id):
    c = _database.cursor()
    c.execute('DELETE FROM movies WHERE movie_id = (?)', (movie_id,))
    c.close()


def watch(movie_id):
    # TODO make this actually increment
    c = _database.cursor()
    c.execute("UPDATE movies SET play_count = play_count + 1 WHERE movie_id = (?)", (movie_id,))
    _database.commit()
    c.close()
    return c.rowcount


def unwatch(movie_id):
    c = _database.cursor()
    c.execute("UPDATE movies SET play_count=? WHERE movie_id = (?)", (0, movie_id))
    _database.commit()
    c.close()
    return c.rowcount


def favor(movie_id):
    c = _database.cursor()
    c.execute("UPDATE movies SET favor=? WHERE movie_id=?", (True, movie_id))
    _database.commit()
    c.close()
    return c.rowcount


def unfavor(movie_id):
    c = _database.cursor()
    c.execute("UPDATE movies SET favor=? WHERE movie_id=?", (False, movie_id))
    _database.commit()
    c.close()
    return c.rowcount


def get_movies(genrefilter=False, actorfilter=False, directorfilter=False, studiofilter=False, yearfilter=False,
               mpaafilter=False, watchedfilter=False, favorfilter=False, alphafilter=False):
    c = _database.cursor()
    if genrefilter:
        genrefilter = '%' + genrefilter + '%'
        return c.execute('SELECT DISTINCT * FROM movies WHERE genres LIKE (?)',
                         (genrefilter,))
    elif mpaafilter:
        return c.execute('SELECT DISTINCT * FROM movies WHERE mpaa = (?)', (mpaafilter,))
    elif actorfilter:
        actorfilter = '%' + actorfilter + '%'
        return c.execute('SELECT DISTINCT * FROM movies WHERE actors LIKE (?)',
                         (actorfilter,))
    elif directorfilter:
        return c.execute('SELECT DISTINCT * FROM movies WHERE directors LIKE (?)',
                         (directorfilter,))
    elif studiofilter:
        return c.execute('SELECT DISTINCT * FROM movies WHERE studio = (?)', (studiofilter,))
    elif yearfilter:
        return c.execute('SELECT DISTINCT * FROM movies WHERE year = (?)', (int(yearfilter),))
    elif watchedfilter:
        return c.execute('SELECT DISTINCT * FROM movies WHERE play_count > 0')
    elif favorfilter:
        return c.execute('SELECT DISTINCT * FROM movies WHERE favor = 1')
    elif alphafilter:
        return c.execute('SELECT DISTINCT * FROM movies WHERE title REGEXP (?)',
                         (alphafilter + '*',))
    else:
        return c.execute('SELECT DISTINCT * FROM movies')


def get_types(col):
    c = _database.cursor()
    items = c.execute('select distinct %s from movies' % col)
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


def update_movies(force=False):
    # Check if we've recently updated and skip
    if not force and not _needs_update():
        return

    dialog = xbmcgui.DialogProgress()
    dialog.create('Refreshing Movie Database')
    dialog.update(0, 'Initializing Movie Scan')

    xml_movies_url = '{0}/tve/xml/category?categoryid=448'.format(db_common.API_DOMAIN)
    data = connection.get_url(xml_movies_url)
    movie_list = BeautifulSoup(data).find('titles').find_all('title', recursive=False)

    # Mark all movies as unfound. This will be updated as we go through
    c = _database.cursor()
    c.execute("UPDATE movies SET in_last_update = 0")
    _database.commit()
    c.close()

    total = len(movie_list)
    count = 0

    for movie in movie_list:
        count += 1
        dialog.update(0, 'Scanned {0} of {1} movies'.format(count, total))

        movie_id = movie['titleid']
        title = movie.find('title').string
        title_sort = movie.find('sorttitle').string
        plot = movie.find('description').string
        year = movie.find('releaseyear').string
        duration = movie.find('duration').string
        mpaa = movie.find('rating').string
        advisories = movie.find('advisories').string

        movie_xml_url = '{0}/tve/xml/title?titleid={1}'.format(db_common.API_DOMAIN, movie_id)
        movie_xml = BeautifulSoup(connection.get_url(movie_xml_url)).find('title')

        poster = None
        thumb = None
        for image in movie_xml.find_all('image'):
            if image['width'] == '260' and image['height'] == '390':
                poster = image.find('url').string
            elif image['width'] == '870' and image['height'] == '423':
                thumb = image.find('url').string

        genre = movie_xml.find('bi').find('sub_section_1').string

        actor_list = []
        actor_and_role_list = []
        director_list = []
        writer_list = []
        for credit in movie_xml.find_all('credit'):
            name = credit.find('creditname').string
            if credit['type'] == 'Director':
                director_list.append(name)
            elif credit['type'] == 'Writer':
                writer_list.append(name)
            elif credit['type'] == 'Actor':
                role = credit.find('role').string
                actor_list.append(name)
                actor_and_role_list.append(name+'|'+role)

        actors = ','.join(actor_list)
        actors_and_roles = ','.join(actor_and_role_list)
        directors = ','.join(director_list)
        writers = ','.join(writer_list)

        studio = None
        legel_xml = movie_xml.find('legal')
        if legel_xml.string:
            regex = re.search('[0-9]{4} (.?) All Rights Reserved', legel_xml.string)
            if regex:
                studio = regex.group(1)

        insert(movie_id, title, title_sort, plot, duration, year, studio, mpaa, advisories, directors, writers,
               actors, actors_and_roles, genre, poster, thumb)

    _set_last_update()

    # Find unfound movies and remove them
    c = _database.cursor()
    c.execute("DELETE FROM movies WHERE in_last_update = 0")
    c.close()


def _needs_update():
    # Update every 15 days
    if 'last_update' in _database_meta:
        # http://forum.kodi.tv/showthread.php?tid=112916
        try:
            last_update = datetime.strptime(_database_meta['last_update'], '%Y-%m-%d')
        except TypeError:
            last_update = datetime(*(time.strptime(_database_meta['last_update'], '%Y-%m-%d')[0:6]))
        return (date.today() - last_update.date()).days > 15

    return True


def _set_last_update():
    _database_meta['last_update'] = date.today().strftime('%Y-%m-%d')
    _write_meta_file()


def _write_meta_file():
    f = open(DB_META_FILE, 'w')
    json.dump(_database_meta, f)
    f.close()


DB_META_FILE = os.path.join(common.__addonprofile__, 'movies.meta')
_database_meta = False
if os.path.exists(DB_META_FILE):
    f = open(DB_META_FILE, 'r')
    _database_meta = json.load(f)
    f.close()
else:
    _database_meta = {}

DB_FILE = os.path.join(common.__addonprofile__, 'movies.db')
if not os.path.exists(DB_FILE):
    _database = sqlite.connect(DB_FILE)
    _database.text_factory = str
    _database.row_factory = sqlite.Row
    create()
else:
    _database = sqlite.connect(DB_FILE)
    _database.text_factory = str
    _database.row_factory = sqlite.Row
