# Copyright (c) 2019, MD2K Center of Excellence
# - Rabin Banjade <rabin.banjde@gmail.com>, Md Azim Ullah <mullah@memphis.edu>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import json
import time
from datetime import datetime

import folium
import numpy as np
import pandas as pd
from branca.element import CssLink, Figure, JavascriptLink, MacroElement
from folium.folium import Map
from folium.utilities import iter_points, none_max, none_min, parse_options
from jinja2 import Template


class TimestampedGeoJson(MacroElement):
    """
    Creates a TimestampedGeoJson plugin from timestamped GeoJSONs to append
    into a map with Map.add_child.
    A geo-json is timestamped if:
    * it contains only features of types LineString, MultiPoint, MultiLineString,
      Polygon and MultiPolygon.
    * each feature has a 'times' property with the same length as the
      coordinates array.
    * each element of each 'times' property is a timestamp in ms since epoch,
      or in ISO string.
    Eventually, you may have Point features with a 'times' property being an
    array of length 1.
    Parameters
    ----------
    data: file, dict or str.
        The timestamped geo-json data you want to plot.
        * If file, then data will be read in the file and fully embedded in
          Leaflet's javascript.
        * If dict, then data will be converted to json and embedded in the
          javascript.
        * If str, then data will be passed to the javascript as-is.
    transition_time: int, default 200.
        The duration in ms of a transition from between timestamps.
    loop: bool, default True
        Whether the animation shall loop.
    auto_play: bool, default True
        Whether the animation shall start automatically at startup.
    add_last_point: bool, default True
        Whether a point is added at the last valid coordinate of a LineString.
    period: str, default "P1D"
        Used to construct the array of available times starting
        from the first available time. Format: ISO8601 Duration
        ex: 'P1M' 1/month, 'P1D' 1/day, 'PT1H' 1/hour, and 'PT1M' 1/minute
    duration: str, default None
        Period of time which the features will be shown on the map after their
        time has passed. If None, all previous times will be shown.
        Format: ISO8601 Duration
        ex: 'P1M' 1/month, 'P1D' 1/day, 'PT1H' 1/hour, and 'PT1M' 1/minute
    Examples
    """
    _template = Template("""
        {% macro script(this, kwargs) %}
            L.Control.TimeDimensionCustom = L.Control.TimeDimension.extend({
                _getDisplayDateFormat: function(date){
                    var newdate = new moment(date);
                    console.log(newdate)
                    return newdate.format("{{this.date_options}}");
                }
            });
            {{this._parent.get_name()}}.timeDimension = L.timeDimension(
                {
                    period: {{ this.period|tojson }},
                }
            );
            var timeDimensionControl = new L.Control.TimeDimensionCustom(
                {{ this.options|tojson }}
            );
            {{this._parent.get_name()}}.addControl(this.timeDimensionControl);
            var geoJsonLayer = L.geoJson({{this.data}}, {
                    pointToLayer: function (feature, latLng) {
                        if (feature.properties.icon == 'marker') {
                            if(feature.properties.iconstyle){
                                return new L.Marker(latLng, {
                                    icon: L.icon(feature.properties.iconstyle)});
                            }
                            //else
                            return new L.Marker(latLng);
                        }
                        if (feature.properties.icon == 'circle') {
                            if (feature.properties.iconstyle) {
                                return new L.circleMarker(latLng, feature.properties.iconstyle)
                                };
                            //else
                            return new L.circleMarker(latLng);
                        }
                        //else
                        return new L.Marker(latLng);
                    },
                    style: function (feature) {
                        return feature.properties.style;
                    },
                    onEachFeature: function(feature, layer) {
                        if (feature.properties.popup) {
                        layer.bindPopup(feature.properties.popup);
                        }
                    }
                })
            var {{this.get_name()}} = L.timeDimension.layer.geoJson(
                geoJsonLayer,
                {
                    updateTimeDimension: true,
                    addlastPoint: {{ this.add_last_point|tojson }},
                    duration: {{ this.duration }},
                }
            ).addTo({{this._parent.get_name()}});
        {% endmacro %}
        """)  # noqa

    def __init__(self, data, transition_time=200, loop=False, auto_play=False,
                 add_last_point=True, period='P1D', min_speed=0.1, max_speed=10,
                 loop_button=False, date_options='YYYY-MM-DD HH:mm:ss',
                 time_slider_drag_update=False, duration=None, speed_slider=True,
                 backward_button=False,forward_button=False,play_reverse_button=False):
        super(TimestampedGeoJson, self).__init__()
        self._name = 'TimestampedGeoJson'

        if 'read' in dir(data):
            self.embed = True
            self.data = data.read()
        elif type(data) is dict:
            self.embed = True
            self.data = json.dumps(data)
        else:
            self.embed = False
            self.data = data
        self.add_last_point = bool(add_last_point)
        self.period = period
        self.date_options = date_options
        self.duration = 'undefined' if duration is None else '"' + duration + '"'

        self.options = parse_options(
            position='bottomleft',
            min_speed=min_speed,
            max_speed=max_speed,
            auto_play=auto_play,
            loop_button=False,
            time_slider_drag_update=time_slider_drag_update,
            speed_slider=speed_slider,
            backward_button=False,
            forward_button=False,
            play_button=False,
            play_reverse_button=False,
            player_options={
                'transitionTime': int(transition_time),
                'loop': loop,
                'startOver': True
            },
        )

    def render(self, **kwargs):
        assert isinstance(self._parent, Map), (
            'TimestampedGeoJson can only be added to a Map object.'
        )
        super(TimestampedGeoJson, self).render()

        figure = self.get_root()
        assert isinstance(figure, Figure), ('You cannot render this Element '
                                            'if it is not in a Figure.')

        figure.header.add_child(
            JavascriptLink('https://cdnjs.cloudflare.com/ajax/libs/jquery/2.0.0/jquery.min.js'),  # noqa
            name='jquery2.0.0')

        figure.header.add_child(
            JavascriptLink('https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.10.2/jquery-ui.min.js'),  # noqa
            name='jqueryui1.10.2')

        figure.header.add_child(
            JavascriptLink('https://rawcdn.githack.com/nezasa/iso8601-js-period/master/iso8601.min.js'),  # noqa
            name='iso8601')

        figure.header.add_child(
            JavascriptLink('https://rawcdn.githack.com/socib/Leaflet.TimeDimension/master/dist/leaflet.timedimension.min.js'),  # noqa
            name='leaflet.timedimension')

        figure.header.add_child(
            CssLink('https://cdnjs.cloudflare.com/ajax/libs/highlight.js/8.4/styles/default.min.css'),  # noqa
            name='highlight.js_css')

        figure.header.add_child(
            CssLink("https://rawcdn.githack.com/socib/Leaflet.TimeDimension/master/dist/leaflet.timedimension.control.min.css"),  # noqa
            name='leaflet.timedimension_css')

        figure.header.add_child(
            JavascriptLink('https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.18.1/moment.min.js'),
            name='moment')

    def _get_self_bounds(self):
        """
        Computes the bounds of the object itself (not including it's children)
        in the form [[lat_min, lon_min], [lat_max, lon_max]].
        """
        if not self.embed:
            raise ValueError('Cannot compute bounds of non-embedded GeoJSON.')

        data = json.loads(self.data)
        if 'features' not in data.keys():
            # Catch case when GeoJSON is just a single Feature or a geometry.
            if not (isinstance(data, dict) and 'geometry' in data.keys()):
                # Catch case when GeoJSON is just a geometry.
                data = {'type': 'Feature', 'geometry': data}
            data = {'type': 'FeatureCollection', 'features': [data]}

        bounds = [[None, None], [None, None]]
        for feature in data['features']:
            for point in iter_points(feature.get('geometry', {}).get('coordinates', {})):  # noqa
                bounds = [
                    [
                        none_min(bounds[0][0], point[1]),
                        none_min(bounds[0][1], point[0]),
                    ],
                    [
                        none_max(bounds[1][0], point[1]),
                        none_max(bounds[1][1], point[0]),
                    ],
                ]
        return bounds


def color(intensity,
          low_threshold=0,
          medium_threshold=10,
          high_threshold=50):
    '''
    sets color intensity based on number of encounters for icon markers
    '''
    col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-yellow.png'
    if low_threshold<= intensity< medium_threshold:
        col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-green.png'
    elif medium_threshold<=intensity<high_threshold:
        col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-yellow.png'
    elif intensity>=high_threshold:
        col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png'
    return col

def color_daily(intensity,
                low_threshold=0,
                medium_threshold=6,
                high_threshold=28):
    '''
    sets color intensity based on number of encounters for icon markers
    '''
    col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-yellow.png'
    if low_threshold<= intensity< medium_threshold:
        col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-green.png'
    elif medium_threshold<=intensity<high_threshold:
        col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-yellow.png'
    elif intensity>=high_threshold:
        col = 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png'
    return col

def get_utcoffset():
    ts = time.time()
    utc_offset = (datetime.utcfromtimestamp(ts) -
                  datetime.fromtimestamp(ts)).total_seconds()/3600
    return utc_offset

def create_geojson_features(df,
                            day,
                            time_column_name='start_time',
                            latitude_columns_name='latitude',
                            longitude_column_name='longitude',
                            visualize_column_name='total_encounters',
                            low_threshold=0,
                            medium_threshold=20,
                            high_threshold=60):
    '''
    creates geojson features for timestamped geojson
    '''
    df[time_column_name] = pd.to_datetime(df[time_column_name])
    lat = 35.162240
    lon = -89.926420
    offset = np.abs(get_utcoffset())
    features = []
    for lat, lan, intensity, time in zip(df[latitude_columns_name], df[longitude_column_name], df[visualize_column_name], df[time_column_name]):
        time = time.timestamp()*1000 + offset*3600*1000
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [lan, lat]
            },
            'properties': {'time':time,'icon':'marker',
                           'iconstyle':{'iconUrl':color(intensity=intensity,low_threshold=low_threshold,
                                                        medium_threshold=medium_threshold,high_threshold=high_threshold),
                                        'iconAnchor':[12.5,41]}}
        }
        features.append(feature)
    html_file = hourly_plotter(features, day, lat, lon)
    return html_file



def hourly_plotter(features,
                   day,
                   lat,
                   lon):
    '''
    plots geojson features with a slider
    '''
    latitude = lat
    longitude = lon
    m = folium.Map([latitude, longitude], zoom_start=10,tiles='https://odin.md2k.org/mcontain_map/tile/{z}/{x}/{y}.png',attr="toner-bcg")

    TimestampedGeoJson(
        {'type': 'FeatureCollection',
         'features': features}
        , period='PT1H'
        , duration='PT59M'
        , add_last_point=True
        , auto_play=False
        , loop=False
        , max_speed=1
        , date_options='MM/DD/YY h:00 A'
        , time_slider_drag_update=True
        , speed_slider=False
        , loop_button=False).add_to(m)

    html_element = '''
                    <div style="position: absolute; text-align: right;
                                top: 10px; right: 10px;
                                z-index:9999;font-size:14px; font-weight:bold;
                                ">{}<br>
                    </div>

                    <div style="position: absolute; text-align: right; 
                                bottom: 15px; right: 10px; 
                                z-index:9999; font-size:14px;font-weight:500;
                                "> 
                                  28 & Up <i class="fa fa-map-marker fa-2x" style="color:#CB2B3E"></i> <br>
                                  6 - 27 <i class="fa fa-map-marker fa-2x" style="color:#FFD326"></i><br>
                                  
                    </div>
                    '''.format("Last updated<br>"+str(day))
    m.get_root().html.add_child(folium.Element(html_element))
    return m

def daily_plotter(df,
                  day,
                  time_column_name='start_time',
                  latitude_columns_name='latitude',
                  longitude_column_name='longitude',
                  visualize_column_name='total_encounters',
                  low_threshold=0,
                  medium_threshold=6,
                  high_threshold=28):
    lat = 35.162240
    lon = -89.926420
    latitude = lat
    longitude = lon
    m = folium.Map([latitude, longitude], zoom_start=10,tiles='https://odin.md2k.org/mcontain_map/tile/{z}/{x}/{y}.png',attr="toner-bcg")
    for lat, lan, intensity in zip(df[latitude_columns_name], df[longitude_column_name], df[visualize_column_name]):
        folium.Marker(location=[lat, lan], icon=folium.features.CustomIcon(icon_image=color_daily(intensity=intensity,
                                                                                                  low_threshold=low_threshold,
                                                                                                  medium_threshold=medium_threshold,
                                                                                                  high_threshold=high_threshold)),icon_anchor=[12.5,
                                                                                                                                               41]).add_to(m)
        html_element = '''
                    <div style="position: absolute; text-align: right;
                                top: 10px; right: 10px; height: 150px; 
                                z-index:9999;font-size:14px; font-weight:bold;
                                ">{}<br>

                    </div>

                    <div style="position: absolute; text-align: right; 
                                bottom: 15px; right: 10px; 
                                z-index:9999; font-size:14px;font-weight:500;
                                "> 
                                  28 & Up <i class="fa fa-map-marker fa-2x" style="color:#CB2B3E"></i> <br>
                                  6 - 27 <i class="fa fa-map-marker fa-2x" style="color:#FFD326"></i><br>
                    </div>



                    '''.format("Last updated<br>"+str(day))

    m.get_root().html.add_child(folium.Element(html_element))
    return m