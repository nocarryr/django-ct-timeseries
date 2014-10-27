import datetime
import json
from django.contrib.auth.decorators import login_required
from django.shortcuts import render_to_response
from django.http import HttpResponse, QueryDict
from django.template import RequestContext
from django.utils import timezone

from ct_timeseries.models import TimeSeries

DEFAULT_TZ = timezone.get_default_timezone()

@login_required
def index(req, **kwargs):
    return render_to_response('ct_timeseries/charts.html', {}, context_instance=RequestContext(req))

def parse_daterange(req=None, tz=None, querydict=None, **kwargs):
    date_range = []
    if querydict is None:
        if req is not None:
            querydict = req.REQUEST
        else:
            querydict = QueryDict('')
    for key in ['start_date', 'end_date']:
        dtstr = kwargs.get(key)
        if not dtstr:
            dtstr = querydict.get(key)
        if not dtstr:
            dt = None
        else:
            dt = datetime.datetime.strptime(dtstr, '%Y-%m-%d')
            if tz is None:
                tz = DEFAULT_TZ
            dt = tz.localize(dt)
        date_range.append(dt)
    if None in date_range:
        if date_range == [None, None]:
            return None
        if date_range[1] is None:
            now = datetime.datetime.now()
            if tz is not None:
                now = tz.localize(now)
            date_range[1] = now
    return date_range
    
def dt_to_jsstr(dt):
    s = dt.isoformat()
    ds, ts = s.split('T')
    def split_tz(_s):
        for c in '-+':
            if c in _s:
                l = _s.split(c)
                l[1] = ''.join([c, l[1]])
                return l
        return _s, ''
    if '.' not in ts:
        ms = '000'
        ts, tzs = split_tz(ts)
    else:
        ts, ms = ts.split('.')
        ms, tzs = split_tz(ms)
    return '%sT%s.%s%s' % (ds, ts, ms, tzs)
    
def get_timeseries_data(req, **kwargs):
    series_id = kwargs.get('series_id')
    if req.method == 'GET':
        qdict = req.GET
    elif req.method == 'POST':
        qdict = req.POST
    else:
        qdict = QueryDict('')
    date_range = parse_daterange(querydict=qdict)
    series = TimeSeries.objects.get(id=series_id)
    data = {}
    data['title'] = series.name
    series_values = {}
    if date_range is None:
        q = series.date_periods.all()
    else:
        q = series.date_periods.filter(date__range=[dt.date() for dt in date_range])
    for date_period in q:
        for time_period in date_period.time_periods.all():
            dt = time_period.datetime_range[0]
            dt_iso = dt_to_jsstr(dt)
            for value_obj in time_period.values.all():
                value_name = value_obj.value_source.name
                if value_name is None:
                    value_name = unicode(value_obj.value_source)
                if value_name not in series_values:
                    series_values[value_name] = {'name':value_name, 'data':[]}
                series_values[value_name]['data'].append([dt_iso, value_obj.value])
    data['series'] = series_values.values()
    return data
    
def get_timeseries_data_json(req, **kwargs):
    data = get_timeseries_data(req, **kwargs)
    s = json.dumps(data)
    return HttpResponse(s, content_type='application/json')
    
    
    
    
