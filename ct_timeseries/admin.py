from django.contrib import admin

from timeseries_ct.models import TimeInterval, \
                                 TimeSeries, \
                                 ValueSource, \
                                 DatePeriod, \
                                 TimePeriod, \
                                 TimeValue

admin.site.register(TimeInterval)
admin.site.register(TimeSeries)
admin.site.register(ValueSource)
admin.site.register(DatePeriod)
admin.site.register(TimePeriod)
admin.site.register(TimeValue)
