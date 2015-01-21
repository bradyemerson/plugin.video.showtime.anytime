#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path
import sys
import string

import xbmc
import xbmcvfs
import xbmcgui
import resources.lib.common as common
import database_tv as tv_db
import database_common as db_common
from bs4 import BeautifulSoup


pluginhandle = common.pluginHandle

if common.get_setting('libraryfolder') == '0':
    MOVIE_PATH = os.path.join(xbmc.translatePath(common.__addonprofile__), 'Movies')
    TV_SHOWS_PATH = os.path.join(xbmc.translatePath(common.__addonprofile__), 'TV')
else:  # == 1
    if common.get_setting('customlibraryfolder') != '':
        MOVIE_PATH = os.path.join(xbmc.translatePath(common.get_setting('customlibraryfolder')), 'Movies')
        TV_SHOWS_PATH = os.path.join(xbmc.translatePath(common.get_setting('customlibraryfolder')), 'TV')
    else:
        # notify of the missing config...
        pass


def setup_library():
    source_path = os.path.join(xbmc.translatePath('special://profile/'), 'sources.xml')
    dialog = xbmcgui.Dialog()

    # ensure the directories exist
    _create_directory(MOVIE_PATH)
    _create_directory(TV_SHOWS_PATH)

    try:
        file = xbmcvfs.File(source_path, 'r')
        content = file.read()
        file.close()
    except:
        # TODO Provide a Yes/No option here
        dialog.ok("Error", "Could not read from sources.xml, does it really exist?")
        return

    soup = BeautifulSoup(content)
    video = soup.find("video")

    added_new_paths = False

    if len(soup.find_all('name', text=db_common.SERVICE_NAME + ' Movies')) < 1:
        movie_source_tag = soup.new_tag('source')

        movie_name_tag = soup.new_tag('name')
        movie_name_tag.string = db_common.SERVICE_NAME + ' Movies'
        movie_source_tag.insert(0, movie_name_tag)

        movie_path_tag = soup.new_tag('path', pathversion='1')
        movie_path_tag.string = MOVIE_PATH
        movie_source_tag.insert(1, movie_path_tag)

        movie_sharing = soup.new_tag('allowsharing')
        movie_sharing.string = 'true'
        movie_source_tag.insert(2, movie_sharing)

        video.append(movie_source_tag)
        added_new_paths = True

    if len(soup.find_all('name', text=db_common.SERVICE_NAME + ' TV')) < 1:
        tv_source_tag = soup.new_tag('source')

        tvshow_name_tag = soup.new_tag('name')
        tvshow_name_tag.string = db_common.SERVICE_NAME + ' TV'
        tv_source_tag.insert(0, tvshow_name_tag)

        tvshow_path_tag = soup.new_tag('path', pathversion='1')
        tvshow_path_tag.string = TV_SHOWS_PATH
        tv_source_tag.insert(1, tvshow_path_tag)

        tvshow_sharing = soup.new_tag('allowsharing')
        tvshow_sharing.string = 'true'
        tv_source_tag.insert(2, tvshow_sharing)

        video.append(tv_source_tag)
        added_new_paths = True

    if added_new_paths:
        file = xbmcvfs.File(source_path, 'w')
        file.write(str(soup))
        file.close()

    return added_new_paths


def update_xbmc_library():
    xbmc.executebuiltin("UpdateLibrary(video)")


def export_movie(data, makeNFO=True):
    if data['year']:
        filename = _clean_filename(data['title'] + ' (' + str(data['year']) + ')')
    else:
        filename = _clean_filename(data['title'])

    strm_file = filename + ".strm"
    u = sys.argv[0] + '?url={0}&mode=movies&sitemode=play_movie'.format(data['movie_id'])
    _save_file(strm_file, u, MOVIE_PATH)

    if makeNFO:
        nfo_file = filename + ".nfo"
        nfo = '<movie>'
        nfo += '<title>' + data['title'] + '</title>'
        if data['year']:
            nfo += '<year>' + str(data['year']) + '</year>'
        if data['plot']:
            nfo += '<outline>' + data['plot'] + '</outline>'
            nfo += '<plot>' + data['plot'] + '</plot>'

        nfo += '<thumb>' + data['poster'] + '</thumb>'
        nfo += '<fanart>' + data['thumb'] + '</fanart>'
        if data['mpaa']:
            nfo += '<mpaa>Rated ' + data['mpaa'] + '</mpaa>'
        if data['studio']:
            nfo += '<studio>' + data['studio'] + '</studio>'
        if data['play_count']:
            nfo += '<playcount>{0}</playcount>'.format(data['play_count'])
        if data['genres']:
            for genre in data['genres'].split(','):
                nfo += '<genre>' + genre + '</genre>'
        if data['directors']:
            nfo += '<director>' + ' / '.join(data['directors'].split(',')) + '</director>'
        if data['writers']:
            nfo += '<writer>' + ' / '.join(data['writers'].split(',')) + '</writer>'
        if data['actors_and_roles']:
            for actor in data['actors_and_roles'].split(','):
                name, role = actor.split('|')
                nfo += '<actor>'
                nfo += '<name>' + name + '</name>'
                nfo += '<role>' + role + '</role>'
                nfo += '</actor>'
        nfo += '</movie>'
        _save_file(nfo_file, nfo, MOVIE_PATH)


def export_series(series):
    dirname = os.path.join(TV_SHOWS_PATH, series['title'].replace(':', ''))
    _create_directory(dirname)

    tv_db.update_series(series['series_id'])

    seasons = tv_db.get_seasons(series['series_id'])
    for season in seasons:
        _export_season(season, dirname)


def _export_season(season, series_dir):
    dirname = os.path.join(series_dir, 'Season {0}'.format(season['season_no']))
    _create_directory(dirname)

    episodes = tv_db.get_episodes(season['season_id'])
    for data in episodes:
        _export_episode(data, dirname)


def _export_episode(data, season_dir, makeNFO=True):
    filename = 'S{0:02d}E{1:02d} - {2}'.format(data['season_no'], data['episode_no'], _clean_filename(data['title']))

    strm_file = filename + ".strm"

    u = sys.argv[0] + '?url={0}&mode=tv&sitemode=play_movie'.format(data['episode_id'])
    _save_file(strm_file, u, season_dir)

    if makeNFO:
        nfo_file = filename + ".nfo"
        nfo = '<episodedetails>'
        nfo += '<title>' + data['title'] + '</title>'
        nfo += '<season>{0}</season>'.format(data['season_no'])
        nfo += '<episode>{0}</episode>'.format(data['episode_no'])
        if data['year']:
            nfo += '<year>' + str(data['year']) + '</year>'
        if data['plot']:
            nfo += '<outline>' + data['plot'] + '</outline>'
            nfo += '<plot>' + data['plot'] + '</plot>'
        if data['duration']:
            nfo += '<runtime>' + str(data['duration']) + '</runtime>'  ##runtime in minutes
        nfo += '<thumb>' + data['thumb'] + '</thumb>'
        if data['mpaa']:
            nfo += '<mpaa>Rated ' + data['mpaa'] + '</mpaa>'
        if data['play_count']:
            nfo += '<playcount>{0}</playcount>'.format(data['play_count'])
        nfo += '</episodedetails>'
        _save_file(nfo_file, nfo, MOVIE_PATH)


def complete_export(added_folders):
    if added_folders:
        xbmcgui.Dialog() \
            .ok("Added {0} Folders to Video Sources".format(db_common.SERVICE_NAME),
                "Two steps are required to complete the process:",
                "1. Kodi must be restarted",
                "2. After restarting, you must configure the content type of the {0} folders in the File section".format(
                    db_common.SERVICE_NAME)
        )
    else:
        common.notification('Export Complete')
        if common.get_setting('updatelibraryafterexport') == 'true':
            update_xbmc_library()


def _save_file(filename, data, dir):
    path = os.path.join(dir, filename)
    file = xbmcvfs.File(path, 'w')
    file.write(data)
    file.close()


def _create_directory(dir_path):
    dir_path = dir_path.strip()
    if not xbmcvfs.exists(dir_path):
        xbmcvfs.mkdir(dir_path)


def _clean_filename(name):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in name if c in valid_chars)
