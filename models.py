import datetime
import pytz
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import ProtectedError
from django.db.models.signals import pre_delete, post_delete
from django.dispatch import receiver
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

_LOG_FILENAME = None
_LOG_ENABLE = False
_LOG_DT_FMT = '%Y%m%d-%H:%M:%S.%f'
def LOG(*args):
    global _LOG_FILENAME, _LOG_ENABLE
    if _LOG_FILENAME is not None:
        if not _LOG_ENABLE:
            _LOG_ENABLE = True
    if not _LOG_ENABLE:
        return
    now = datetime.datetime.now()
    entry = [now.strftime(_LOG_DT_FMT)]
    entry.extend([str(arg) for arg in args])
    entry = '\t'.join(entry)
    if _LOG_FILENAME == 'stdout':
        print entry
    else:
        with open(_LOG_FILENAME, 'a') as f:
            f.write('%s\t' % (entry))
    

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
    interval = models.ForeignKey(TimeInterval, related_name='time_series')
    @property
    def interval_gte_day(self):
        v = getattr(self, '_interval_gte_day', None)
        if v is None:
            period = self.interval.period
            v = self._interval_gte_day = period >= 86400
        return v
    def get_next_datetime(self, start_dt=None, return_if_equal=False):
        original_start_dt = start_dt
        if start_dt is None:
            if self.date_periods.count():
                start_date = self.date_periods.latest('date').date + DAY
                start_dt = UTC.localize(datetime.datetime.combine(start_date, MIDNIGHT))
        elif isinstance(start_dt, datetime.date):
            start_dt = UTC.localize(datetime.datetime.combine(start_dt, MIDNIGHT))
            original_start_dt = start_dt
        vobj_dt = []
        for value_source in self.value_sources.all():
            dt = value_source.get_next_datetime(start_dt)
            if dt is None:
                continue
            if dt.tzinfo is None:
                dt = UTC.localize(dt)
            else:
                dt = UTC.normalize(dt)
            vobj_dt.append(dt)
        if not len(vobj_dt):
            return None
        if start_dt is not None:
            LOG('start_dt: %s' % (start_dt))
            next_dt = None
            for dt in sorted(vobj_dt):
                if dt.date() < start_dt.date():
                    continue
                next_dt = dt
                LOG('next_dt from vobj: %s' % (next_dt))
                break
            if next_dt is None:
                next_dt = min(vobj_dt)
            if original_start_dt is not None:
                if next_dt.date() < original_start_dt.date():
                    next_dt = original_start_dt.date() + DAY
                    LOG('next_dt(lt): %s' % (next_dt))
                    return self.get_next_datetime(next_dt)
            return next_dt
        else:
            next_dt = min(vobj_dt)
        LOG('next_dt: %s' % (next_dt))
        if self.date_periods.filter(date=next_dt.date()).exists():
            next_dt += DAY
            LOG('next_dt(in_db): %s' % (next_dt))
            return self.get_next_datetime(next_dt)
        LOG('next_dt(no_change): %s' % (next_dt))
        return next_dt
    def add_date_period(self, date=None):
        if date is None:
            dt = self.get_next_datetime()
            LOG('retrieved %s' % (dt))
        if dt is None:
            return False
        date = dt.date()
        q = DatePeriod.objects.filter(series=self)
        if q.filter(date=date).exists():
            LOG('add_date_period: date %s exists' % (date))
            while q.filter(date=date).exists():
                date += DAY
                dt = self.get_next_datetime(date)
                if dt is None:
                    return False
                date = dt.date()
            LOG('add_date_period: new date: %s' % (date))
        dobj = DatePeriod(series=self, date=date)
        dobj.save()
        LOG('%s building time_periods' % (dobj))
        dobj.build_time_periods()
        if dobj.time_periods.count():
            LOG('*******************')
        else:
            LOG('-------------------')
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
@receiver(pre_delete, sender=TimeInterval)
def on_timeinterval_pre_delete(sender, **kwargs):
    time_interval = kwargs.get('instance')
    q = time_interval.time_series.all()
    if q.exists():
        msg = 'Cannot delete TimeInterval "%s". It is in use by the following TimeSeries objects: %s'
        msg = msg % (time_interval, list(q))
        raise ProtectedError(msg, q)
    
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
    def get_next_datetime(self, start_dt=None):
        lookup = getattr(self.source_model.model_class(), self.next_valid_date_lookup)
        return lookup(start_dt)
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
            del pobj
    def __unicode__(self):
        return unicode(self.date)
    
@receiver(pre_delete, sender=TimeSeries)
def on_timeseries_pre_delete(sender, **kwargs):
    time_series = kwargs.get('instance')
    time_series.date_periods.all().delete()
    time_series.value_sources.all().delete()
    
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
            del vobj
    def __unicode__(self):
        return u'-'.join([unicode(dt) for dt in self.datetime_range])

@receiver(pre_delete, sender=DatePeriod)
def on_dateperiod_pre_delete(sender, **kwargs):
    date_period = kwargs.get('instance')
    date_period.time_periods.all().delete()
    
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

@receiver(pre_delete, sender=TimePeriod)
def on_timeperiod_pre_delete(sender, **kwargs):
    time_period = kwargs.get('instance')
    time_period.values.all().delete()
    
@receiver(pre_delete, sender=ValueSource)
def on_valuesource_pre_delete(sender, **kwargs):
    value_source = kwargs.get('instance')
    value_source.values.all().delete()
    
@receiver(post_delete, sender=TimeValue)
def on_timevalue_post_delete(sender, **kwargs):
    time_value = kwargs.get('instance')
    try:
        period = time_value.period
    except TimePeriod.DoesNotExist:
        return
    if not period.values.exclude(id=time_value.id).exists():
        period.delete()

def update_series(queryset=None, log_filename=None):
    global _LOG_FILENAME
    if log_filename is not None:
        _LOG_FILENAME = log_filename
    if queryset is None:
        queryset = TimeSeries.objects.all()
    for series in queryset:
        series.update_data()
