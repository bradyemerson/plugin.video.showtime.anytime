#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import urllib

import xbmcplugin
import xbmc
import xbmcgui
import common
import database_tv as tv_db
import database_common


pluginhandle = common.pluginHandle

# 501-POSTER WRAP 503-MLIST3 504=MLIST2 508-FANARTPOSTER 
confluence_views = [500, 501, 502, 503, 504, 508]

###################### Television

def list_tv_root():
    tv_db.update_tv(False)

    cm_u = sys.argv[0] + '?mode=tv&sitemode=list_tvshows_favor_filtered_export&url=""'
    cm = [('Export Favorites to Library', 'XBMC.RunPlugin(%s)' % cm_u)]
    common.add_directory('Favorites', 'tv', 'list_tvshows_favor_filtered', contextmenu=cm)

    cm_u = sys.argv[0] + '?mode=tv&sitemode=list_tvshows_export&url=""'
    cm = [('Export All to Library', 'XBMC.RunPlugin(%s)' % cm_u)]
    common.add_directory('All Shows', 'tv', 'list_tvshows', contextmenu=cm)

    # common.add_directory('Genres', 'tv', 'list_tvshow_types', 'GENRE')
    # common.add_directory('TV Rating', 'tv', 'list_tvshow_types', 'MPAA')
    # common.add_directory('Actors', 'tv', 'list_tvshow_types', 'ACTORS')
    # common.add_directory('Watched', 'tv', 'list_tvshows_watched_filtered')
    xbmcplugin.endOfDirectory(pluginhandle)


def list_tvshow_types(type=False):
    if not type:
        type = common.args.url

    if type == 'GENRE':
        mode = 'list_tvshows_genre_filtered'
        items = tv_db.get_types('genres')
    elif type == 'MPAA':
        mode = 'list_tvshows_mpaa_filtered'
        items = tv_db.get_types('mpaa')
    elif type == 'ACTORS':
        mode = 'list_tvshows_actors_filtered'
        items = tv_db.get_types('actors')

    for item in items:
        common.add_directory(item, 'tv', mode, item)

    xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_LABEL)
    xbmcplugin.endOfDirectory(pluginhandle)


def list_tvshows_genre_filtered():
    list_tvshows(export=False, genrefilter=common.args.url)


def list_tvshows_mpaa_filtered():
    list_tvshows(export=False, mpaafilter=common.args.url)


def list_tvshows_creators_filtered():
    list_tvshows(export=False, creatorfilter=common.args.url)


def list_tvshows_favor_filtered_export():
    list_tvshows(export=True, favorfilter=True)


def list_tvshows_favor_filtered():
    list_tvshows(export=False, favorfilter=True)


def list_tvshows_export():
    list_tvshows(export=True)


def list_tvshows(export=False, mpaafilter=False, genrefilter=False, creatorfilter=False, favorfilter=False):
    if export:
        import xbmclibrary

        added_folders = xbmclibrary.setup_library()

    shows = tv_db.get_series(favorfilter=favorfilter).fetchall()
    total = len(shows)

    for showdata in shows:
        if export:
            xbmclibrary.export_series(showdata)
        else:
            _add_series_item(showdata, total)

    if export:
        xbmclibrary.complete_export(added_folders)

    else:
        xbmcplugin.setContent(pluginhandle, 'tvshows')
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_VIDEO_SORT_TITLE_IGNORE_THE)
        xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_VIDEO_YEAR)
        # xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_MPAA_RATING)
        #xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_STUDIO_IGNORE_THE)
        xbmcplugin.endOfDirectory(pluginhandle)

        viewenable = common.get_setting("viewenable")
        if viewenable == 'true':
            view = int(common.get_setting("showview"))
            xbmc.executebuiltin("Container.SetViewMode(" + str(confluence_views[view]) + ")")


def _add_series_item(data, total=0):
    watched_episodes = tv_db.get_series_episode_count(data['series_id'], 'watched')

    labels = {
        'title': data['title'],
        'tvshowtitle': data['title'],
        'sorttitle': data['title_sort'],
        'plot': data['plot'],
        'episode': data['total_episodes'],
        'season': data['total_seasons'],
        'year': tv_db.get_series_year(data['series_id'])
    }

    item = xbmcgui.ListItem(data['title'], iconImage=data['thumb'], thumbnailImage=data['thumb'])
    item.setInfo(type='Video', infoLabels=labels)
    item.setProperty('fanart_image', data['thumb'])
    item.setProperty('TVShowThumb', data['thumb'])
    item.setProperty('TotalSeasons', str(data['total_seasons']))
    item.setProperty('TotalEpisodes', str(data['total_episodes']))
    item.setProperty('WatchedEpisodes', str(watched_episodes))
    item.setProperty('UnWatchedEpisodes', str(int(data['total_seasons']) - watched_episodes))

    contextmenu = []
    if data['favor']:
        cm_u = sys.argv[0] + '?url={0}&mode=tv&sitemode=unfavor_series&title={1}'.format(data['series_id'],
                                                                                         urllib.unquote_plus(
                                                                                             data['title']))
        contextmenu.append((common.localise(39006).format(database_common.SERVICE_NAME), 'XBMC.RunPlugin(%s)' % cm_u))
    else:
        cm_u = sys.argv[0] + '?url={0}&mode=tv&sitemode=favor_series&title={1}'.format(data['series_id'],
                                                                                       urllib.unquote_plus(
                                                                                           data['title']))
        contextmenu.append((common.localise(39007).format(database_common.SERVICE_NAME), 'XBMC.RunPlugin(%s)' % cm_u))

    contextmenu.append(('TV Show Information', 'XBMC.Action(Info)'))

    item.addContextMenuItems(contextmenu)

    u = sys.argv[0] + '?url={0}&mode=tv&sitemode=list_tv_seasons'.format(data['series_id'])
    xbmcplugin.addDirectoryItem(pluginhandle, url=u, listitem=item, isFolder=True, totalItems=total)


def list_tv_seasons():
    series_id = common.args.url

    tv_db.update_series(series_id)

    seasons = tv_db.get_seasons(series_id).fetchall()
    total = len(seasons)

    for season in seasons:
        _add_season_item(season, total)

    xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_LABEL)
    xbmcplugin.setContent(pluginhandle, 'tvshows')
    xbmcplugin.endOfDirectory(pluginhandle)

    viewenable = common.get_setting("viewenable")
    if viewenable == 'true':
        view = int(common.get_setting("seasonview"))
        xbmc.executebuiltin("Container.SetViewMode(" + str(confluence_views[view]) + ")")


def _add_season_item(data, total=0):
    total_episodes = tv_db.get_season_episode_count(data['season_id'])
    watched_episodes = tv_db.get_season_episode_count(data['season_id'], 'watched')
    title = 'Season {0}'.format(data['season_no'])

    labels = {
        'title': title,
        'tvshowtitle': data['series_title'],
        'season': data['season_no'],
        'episode': total_episodes,
        'year': tv_db.get_season_year(data['season_id'])
    }

    item = xbmcgui.ListItem(title)
    item.setInfo(type='Video', infoLabels=labels)
    item.setProperty('TotalEpisodes', str(total_episodes))
    item.setProperty('WatchedEpisodes', str(watched_episodes))
    item.setProperty('UnWatchedEpisodes', str(total_episodes - watched_episodes))

    u = sys.argv[0] + '?url={0}&mode=tv&sitemode=list_episodes'.format(data['season_id'])
    xbmcplugin.addDirectoryItem(pluginhandle, url=u, listitem=item, isFolder=True, totalItems=total)


def list_episodes(export=False):
    season_id = common.args.url

    episodes = tv_db.get_episodes(season_id).fetchall()
    total = len(episodes)

    for episode in episodes:
        _add_episode_item(episode, total)

    xbmcplugin.addSortMethod(pluginhandle, xbmcplugin.SORT_METHOD_VIDEO_SORT_TITLE)
    xbmcplugin.setContent(pluginhandle, 'Episodes')
    xbmcplugin.endOfDirectory(pluginhandle)

    viewenable = common.get_setting("viewenable")
    if viewenable == 'true':
        view = int(common.get_setting("episodeview"))
        xbmc.executebuiltin("Container.SetViewMode(" + str(confluence_views[view]) + ")")


def _add_episode_item(data, total):
    labels = {
        'title': data['title'],
        'sorttitle': data['title_sort'],
        'tvshowtitle': data['series_title'],
        'plot': data['plot'],
        'season': data['season_no'],
        'episode': data['episode_no'],
        'year': data['year'],
        'duration': data['duration'],
        'playcount': data['play_count']
    }

    if data['mpaa']:
        labels['mpaa'] = 'Rated ' + data['mpaa']

    item = xbmcgui.ListItem(data['title'], data['mpaa'], iconImage=data['thumb'], thumbnailImage=data['thumb'])
    item.setInfo(type='Video', infoLabels=labels)
    item.setProperty('fanart_image', data['thumb'])

    contextmenu = []

    if data['play_count'] > 0:
        cm_u = sys.argv[0] + '?url={0}&mode=tv&sitemode=unwatch_episode'.format(data['episode_id'])
        contextmenu.append(('Mark as unwatched', 'XBMC.RunPlugin(%s)' % cm_u))
    else:
        cm_u = sys.argv[0] + '?url={0}&mode=tv&sitemode=watch_episode'.format(data['episode_id'])
        contextmenu.append(('Mark as watched', 'XBMC.RunPlugin(%s)' % cm_u))

    contextmenu.append(('Episode Information', 'XBMC.Action(Info)'))

    item.addContextMenuItems(contextmenu)

    u = sys.argv[0] + '?url={0}&mode=tv&sitemode=play_movie'.format(data['episode_id'])
    xbmcplugin.addDirectoryItem(pluginhandle, url=u, listitem=item, isFolder=False, totalItems=total)


def play_movie():
    episode_id = common.args.url

    if tv_db.watch_episode(episode_id) > 0:
        common.refresh_menu()

    url = '{0}/#/episode/{1}'.format(database_common.WEB_DOMAIN, episode_id)
    common.play_url(url)


##########################################
# Context Menu Links
##########################################
def refresh_db():
    tv_db.update_tv(True)


def favor_series():
    content_id = common.args.url
    if tv_db.favor_series(content_id) > 0:
        common.notification('Added ' + urllib.unquote_plus(common.args.title) + ' to favorites')
        common.refresh_menu()
    else:
        common.notification('Error adding movie to favorites', isError=True)


def unfavor_series():
    content_id = common.args.url
    if tv_db.unfavor_series(content_id) > 0:
        common.notification('Removed ' + urllib.unquote_plus(common.args.title) + ' from favorites')
        common.refresh_menu()
    else:
        common.notification('Error removing movie from favorites', isError=True)


def watch_episode():
    content_id = common.args.url
    if tv_db.watch_episode(content_id) > 0:
        common.refresh_menu()
    else:
        common.notification('Could not update watch count', isError=True)


def unwatch_episode():
    content_id = common.args.url
    tv_db.unwatch_episode(content_id)
    common.refresh_menu()