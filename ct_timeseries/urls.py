from django.conf.urls import patterns, url

urlpatterns = patterns('', 
    url(r'^$', 'ct_timeseries.views.index'), 
    url(r'^timeseries/jsondata/(?P<series_id>\d+)/$', 'ct_timeseries.views.get_timeseries_data_json'), 
)
