import datetime
import pytz
from django.core.exceptions import ValidationError
from django.db import models
from django.contrib.contenttypes.models import ContentType

MINUTE = datetime.timedelta(minutes=1)
SECOND = datetime.timedelta(seconds=1)
DAY = datetime.timedelta(days=1)
MIDNIGHT = datetime.time()
UTC = pytz.utc

INTERVAL_MAP = {
    'seconds':1, 
    'minutes':60, 
    'hours':3600, 
}
class TimeInterval(models.Model):
    name = models.CharField(max_length=30, unique=True)
    interval_unit = models.CharField(
        max_length=10, 
        choices=(
            ('seconds', 'Seconds'), 
            ('minutes', 'Minutes'), 
            ('hours', 'Hours'), 
        ),
        default='minutes')
    interval_value = models.FloatField(default=1.)
    @property
    def period(self):
        period = getattr(self, '_period', None)
        if period is None:
            unit = self.interval_unit
            interval = self.interval_value
            period = self._period = float(INTERVAL_MAP[unit]) * interval
        return period
    def iter_periods(self):
        p = self.period
        i = 0
        v = 0
        while v <= 86400:
            v = i * p
            yield i, v
            i += 1
    def calc_seconds(self, index=None):
        if index is None:
            index = 1
        return self.period * index
    def get_datetime_range(self, date, index):
        s = self.calc_seconds(index)
        if s > int(s):
            ms = int((s - int(s)) * 1000000)
            s = int(s)
        else:
            s = int(s)
            ms = 0
        td = datetime.timedelta(seconds=s, microseconds=ms)
        start_dt = UTC.localize(datetime.datetime.combine(date, MIDNIGHT))
        start_dt += td
        end_dt = start_dt + td
        return [start_dt, end_dt]
    def clean(self):
        if self.interval_value == 0:
            raise ValidationError('interval_value must be a positive number')
    def __unicode__(self):
        return self.name
        
class TimeSeries(models.Model):
    name = models.CharField(max_length=100, blank=True, null=True)
    interval = models.ForeignKey(TimeInterval)
    @property
    def interval_gte_day(self):
        v = getattr(self, '_interval_gte_day', None)
        if v is None:
            period = self.interval.period
            v = self._interval_gte_day = period >= 86400
        return v
    def get_next_date(self, start_date=None):
        if start_date is None:
            if self.date_periods.count():
                start_date = self.date_periods.latest('date').date
        vobj_dates = []
        for value_source in self.value_sources.all():
            d = value_source.get_next_date(start_date)
            if d is None:
                continue
            if isinstance(d, datetime.datetime):
                if d.tzinfo is None:
                    d = UTC.localize(d)
                d = d.date()
            vobj_dates.append(d)
        if not len(vobj_dates):
            return None
        next_date = min(vobj_dates)
        if self.date_periods.filter(date=next_date).exists():
            next_date += DAY
        return next_date
    def add_date_period(self, date=None):
        if date is None:
            date = self.get_next_date()
        if date is None:
            return False
        dobj = DatePeriod(series=self, date=date)
        dobj.save()
        dobj.build_time_periods()
        if not dobj.time_periods.count():
            dobj.delete()
        return True
    def update_data(self):
        complete = False
        while not complete:
            r = self.add_date_period()
            if r is False:
                complete = True
    def __unicode__(self):
        if self.name:
            return self.name
        return super(TimeSeries, self).__unicode__()
    
VALUE_TYPE_MAP = {
    'int':int, 
    'float':float, 
    'str':str, 
}

class ValueSource(models.Model):
    name = models.CharField(max_length=100, blank=True, null=True)
    time_series = models.ForeignKey(TimeSeries, related_name='value_sources')
    source_model = models.ForeignKey(ContentType)
    value_type = models.CharField(max_length=5, 
                                  choices=(
                                    ('int', 'int'), 
                                    ('float', 'float'), 
                                    ('str', 'str'), 
                                  ), default='int')
    value_lookup = models.CharField(max_length=100)
    value_lookup_extra_args = models.CharField(max_length=100, blank=True, null=True)
    next_valid_date_lookup = models.CharField(max_length=100)
    def get_value_for_datetime_range(self, dtrange):
        vlookup = getattr(self.source_model.model_class(), self.value_lookup)
        args = [dtrange]
        extra_args = self.value_lookup_extra_args
        if extra_args:
            if ',' in extra_args:
                extra_args = [arg.strip() for arg in args.split(',')]
            args.extend(extra_args)
        return vlookup(*args)
    def get_next_date(self, start_date=None):
        lookup = getattr(self.source_model.model_class(), self.next_valid_date_lookup)
        return lookup(start_date)
    def __unicode__(self):
        if self.name is None:
            if self.source_model is not None:
                return self.source_model.name
            return super(ValueSource, self).__unicode__()
        return unicode(self.name)
    
    
class DatePeriod(models.Model):
    series = models.ForeignKey(TimeSeries, related_name='date_periods')
    date = models.DateField()
    class Meta:
        ordering = ['date']
    def build_time_periods(self):
        for index, period in self.series.interval.iter_periods():
            try:
                pobj = self.time_periods.get(time_index=index)
            except TimePeriod.DoesNotExist:
                pobj = TimePeriod(date_period=self, time_index=index)
                pobj.save()
            pobj.build_values()
            if not pobj.values.count():
                pobj.delete()
    def __unicode__(self):
        return unicode(self.date)
    
class TimePeriod(models.Model):
    date_period = models.ForeignKey(DatePeriod, related_name='time_periods')
    time_index = models.PositiveIntegerField()
    class Meta:
        ordering = ['date_period', 'time_index']
    @property
    def series(self):
        return self.date_period.series
    @property
    def interval(self):
        return self.series.interval
    @property
    def datetime_range(self):
        dtrange = getattr(self, '_datetime_range', None)
        if dtrange is not None:
            return dtrange
        dtrange = self._datetime_range = self.get_datetime_range()
        return dtrange
    def get_datetime_range(self):
        return self.interval.get_datetime_range(self.date_period.date, self.time_index)
    def build_values(self):
        for value_source in self.series.value_sources.all():
            try:
                vobj = self.values.get(value_source=value_source)
            except TimeValue.DoesNotExist:
                vobj = TimeValue(period=self, value_source=value_source)
                vobj.save()
            if vobj.db_value is None:
                vobj.delete()
    def __unicode__(self):
        return u'-'.join([unicode(dt) for dt in self.datetime_range])

class TimeValue(models.Model):
    period = models.ForeignKey(TimePeriod, related_name='values')
    value_source = models.ForeignKey(ValueSource, related_name='values')
    db_value = models.CharField(max_length=100, null=True)
    class Meta:
        ordering = ['period']
    @property
    def value(self):
        if not hasattr(self, '_value'):
            self._value = self.db_value_to_value()
        return self._value
    def db_value_to_value(self, value=None):
        value_type = VALUE_TYPE_MAP[self.value_source.value_type]
        if value is None:
            value = self.db_value
        if value is None and value_type in [int, float]:
            value = 0
        return value_type(value)
    def value_to_db_value(self, value):
        if value is None:
            return value
        return str(value)
    def get_value_from_source(self, do_save=True):
        dtrange = self.period.datetime_range
        value = self.value_source.get_value_for_datetime_range(dtrange)
        db_value = self.db_value = self.value_to_db_value(value)
        self._value = self.db_value_to_value(db_value)
        if do_save:
            self.save()
    def save(self, *args, **kwargs):
        def do_save():
            super(TimeValue, self).save(*args, **kwargs)
        if self.pk is None:
            do_save()
        if self.db_value is None:
            self.get_value_from_source(do_save=False)
        do_save()
    def __unicode__(self):
        name = self.value_source.name
        if name is None:
            name = self.value_source.source_model.model_class().__name__
        return u': '.join([name, unicode(self.db_value)])
    

def update_series(queryset=None):
    if queryset is None:
        queryset = TimeSeries.objects.all()
    for series in queryset:
        series.update_data()
