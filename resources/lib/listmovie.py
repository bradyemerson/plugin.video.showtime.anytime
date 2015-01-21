#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import urllib

import xbmcplugin
import xbmc
import xbmcgui
import common
import database_movies as movies_db
import database_common


pluginhandle = common.pluginHandle

# 501-POSTER WRAP 503-MLIST3 504=MLIST2 508-FANARTPOSTER 
confluence_views = [500, 501, 502, 503, 504, 508]

################################ Movie listing
def list_movie_root():
    movies_db.update_movies(False)

    cm = []
    if common.get_setting('enablelibrary') == 'true':
        cm_u = sys.argv[0] + '?mode=movies&sitemode=list_movies_favor_filtered_export'
        cm = [('Export Favorites to Library', 'XBMC.RunPlugin(%s)' % cm_u)]

    common.add_directory('Favorites', 'movies', 'list_movies_favor_filtered', contextmenu=cm)

    cm = []
    if common.get_setting('enablelibrary') == 'true':
        cm_u = sys.argv[0] + '?mode=movies&sitemode=list_movies_export'
        cm = [('Export All to Library', 'XBMC.RunPlugin(%s)' % cm_u)]

    common.add_directory('All Movies', 'movies', 'list_movies', contextmenu=cm)

    common.add_directory('Genres', 'movies', 'list_movie_types', 'GENRE')
    common.add_directory('Years', 'movies', 'list_movie_types', 'YEARS')
    common.add_directory('Studios', 'movies', 'list_movie_types', 'STUDIOS')
    common.add_directory('MPAA Rating', 'movies', 'list_movie_types', 'MPAA')
    common.add_directory('Directors', 'movies', 'list_movie_types', 'DIRECTORS')
    common.add_directory('Actors', 'movies', 'list_movie_types', 'ACTORS')
    # common.add_directory('Recently Added', 'movies', 'list_movies_recent_filtered', 'ACTORS')
    common.add_directory('Watched', 'movies', 'list_movies_watched_filtered')
    xbmcplugin.endOfDirectory(pluginhandle)


def list_movie_types(type=False):
    if not type:
        type = common.args.url
    if type == 'GENRE':
        mode = 'list_movies_genre_filtered'
        items = movies_db.get_types('genres')
    elif type == 'STUDIOS':
        mode = 'list_movies_studio_filtered'
        items = movies_db.get_types('studio')
    elif type == 'YEARS':
        mode = 'list_movies_year_filtered'
        items = movies_db.get_types('year')
    elif type == 'DIRECTORS':
        mode = 'list_movies_director_filtered'
        items = movies_db.get_types('director')
    elif type == 'MPAA':
        mode = 'list_movies_mpaa_filtered'
        items = movies_db.get_types('mpaa')
    elif type == 'ACTORS':
        mode = 'list_movies_actor_filtered'
        items = movies_db.get_types('actors')

    for item in items:
        common.add_directory(item, 'movies', mode, item)

    xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_LABEL)
    xbmcplugin.endOfDirectory(pluginhandle, updateListing=False)


def list_movies_genre_filtered():
    list_movies(export=False, genrefilter=common.args.url)


def list_movies_year_filtered():
    list_movies(export=False, yearfilter=common.args.url)


def list_movies_mpaa_filtered():
    list_movies(export=False, mpaafilter=common.args.url)


def list_movies_studio_filtered():
    list_movies(export=False, studiofilter=common.args.url)


def list_movies_director_filtered():
    list_movies(export=False, directorfilter=common.args.url)


def list_movies_actor_filtered():
    list_movies(export=False, actorfilter=common.args.url)


def list_movies_watched_filtered():
    list_movies(export=False, watchedfilter=True)


def list_movies_recent_filtered():
    list_movies(export=False, watchedfilter=True)


def list_movies_favor_filtered():
    list_movies(export=False, favorfilter=True)


def list_movies_favor_filtered_export():
    list_movies(export=True, favorfilter=True)


def list_movies_export():
    list_movies(export=True)


def list_movies(export=False, genrefilter=False, actorfilter=False, directorfilter=False, studiofilter=False,
                yearfilter=False, mpaafilter=False, watchedfilter=False, favorfilter=False, alphafilter=False):
    if export:
        import xbmclibrary

        added_folders = xbmclibrary.setup_library()

    movies = movies_db.get_movies(genrefilter=genrefilter, actorfilter=actorfilter, directorfilter=directorfilter,
                                 studiofilter=studiofilter, yearfilter=yearfilter, mpaafilter=mpaafilter,
                                 watchedfilter=watchedfilter, favorfilter=favorfilter,
                                 alphafilter=alphafilter).fetchall()
    total = len(movies)
    for moviedata in movies:
        if export:
            xbmclibrary.export_movie(moviedata)
        else:
            _add_movie_item(moviedata, total)

    if export:
        xbmclibrary.complete_export(added_folders)

    else:
        xbmcplugin.setContent(pluginhandle, 'Movies')
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_VIDEO_SORT_TITLE_IGNORE_THE)
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_VIDEO_YEAR)
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_VIDEO_RUNTIME)
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_MPAA_RATING)
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_DURATION)
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_STUDIO_IGNORE_THE)
        xbmcplugin.endOfDirectory(pluginhandle)

        viewenable = common.get_setting("viewenable")
        if viewenable == 'true' and common.get_setting("movieview"):
            view = int(common.get_setting("movieview"))
            xbmc.executebuiltin("Container.SetViewMode(" + str(confluence_views[view]) + ")")


def _add_movie_item(data, total=0):
    labels = {
        'title': data['title'],
        'sorttitle': data['title_sort'],
        'year': data['year'],
        'studio': data['studio'],
        'duration': data['duration'],
        'playcount': data['play_count'],
        'plot': data['plot']
    }

    if data['mpaa']:
        labels['mpaa'] = 'Rated ' + data['mpaa']
    if data['directors']:
        labels['director'] = ' / '.join(data['directors'].split(','))
    if data['genres']:
        labels['genre'] = ' / '.join(data['genres'].split(','))
    if data['actors']:
        labels['cast'] = data['actors'].split(',')
    if data['actors_and_roles']:
        labels['castandrole'] = data['actors_and_roles'].split(',')
    if data['writers']:
        labels['writer'] = ' / '.join(data['writers'].split(','))

    item = xbmcgui.ListItem(data['title'], data['mpaa'], iconImage=data['poster'], thumbnailImage=data['poster'])
    item.setInfo(type='Video', infoLabels=labels)
    item.setProperty('fanart_image', data['thumb'])

    contextmenu = []
    if data['favor']:
        cm_u = sys.argv[0] + '?url={0}&mode=movies&sitemode=unfavor&title={1}'.format(data['movie_id'],
                                                                                      urllib.unquote_plus(
                                                                                          data['title']))
        contextmenu.append((common.localise(39006).format(database_common.SERVICE_NAME), 'XBMC.RunPlugin(%s)' % cm_u))
    else:
        cm_u = sys.argv[0] + '?url={0}&mode=movies&sitemode=favor&title={1}'.format(data['movie_id'],
                                                                                    urllib.unquote_plus(data['title']))
        contextmenu.append((common.localise(39007).format(database_common.SERVICE_NAME), 'XBMC.RunPlugin(%s)' % cm_u))

    if data['play_count'] > 0:
        cm_u = sys.argv[0] + '?url={0}&mode=movies&sitemode=unwatch'.format(data['movie_id'])
        contextmenu.append(('Mark as unwatched', 'XBMC.RunPlugin(%s)' % cm_u))
    else:
        cm_u = sys.argv[0] + '?url={0}&mode=movies&sitemode=watch'.format(data['movie_id'])
        contextmenu.append(('Mark as watched', 'XBMC.RunPlugin(%s)' % cm_u))

    contextmenu.append(('Movie Information', 'XBMC.Action(Info)'))

    item.addContextMenuItems(contextmenu)

    u = sys.argv[0] + '?url={0}&mode=movies&sitemode=play_movie'.format(data['movie_id'])

    xbmcplugin.addDirectoryItem(pluginhandle, url=u, listitem=item, isFolder=False, totalItems=total)


def play_movie():
    movie_id = common.args.url

    if movies_db.watch(movie_id) > 0:
        common.refresh_menu()

    url = '{0}/#/movie/{1}'.format(database_common.WEB_DOMAIN, movie_id)
    common.play_url(url)


##########################################
# Context Menu Links
##########################################
def refresh_db():
    movies_db.update_movies(True)


def favor():
    content_id = common.args.url
    if movies_db.favor(content_id) > 0:
        common.notification('Added ' + urllib.unquote_plus(common.args.title) + ' to favorites')
        common.refresh_menu()
    else:
        common.notification('Error adding movie to favorites', isError=True)


def unfavor():
    content_id = common.args.url
    if movies_db.unfavor(content_id) > 0:
        common.notification('Removed ' + urllib.unquote_plus(common.args.title) + ' from favorites')
        common.refresh_menu()
    else:
        common.notification('Error removing movie from favorites', isError=True)


def watch():
    content_id = common.args.url
    if movies_db.watch(content_id) > 0:
        common.refresh_menu()
    else:
        common.notification('Could not update watch count', isError=True)


def unwatch():
    content_id = common.args.url
    movies_db.unwatch(content_id)
    common.refresh_menu()

        

