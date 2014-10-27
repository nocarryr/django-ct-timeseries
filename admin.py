from django.contrib import admin
from django.contrib.contenttypes.models import ContentType

from ct_timeseries.models import TimeInterval, \
                                 TimeSeries, \
                                 ValueSource, \
                                 DatePeriod, \
                                 TimePeriod, \
                                 TimeValue

class ValueSourceInline(admin.StackedInline):
    model = ValueSource
    extra = 1
    def formfield_for_foreignkey(self, db_field, req=None, **kwargs):
        field = super(ValueSourceInline, self).formfield_for_foreignkey(db_field, req, **kwargs)
        if db_field.rel.to == ContentType:
            field.label_from_instance = self.get_content_type_label
        return field
    def get_content_type_label(self, content_type):
        return '.'.join([content_type.app_label, content_type.model_class().__name__])
        
class TimeSeriesAdmin(admin.ModelAdmin):
    inlines = [ValueSourceInline, ]
    
admin.site.register(TimeInterval)
admin.site.register(TimeSeries, TimeSeriesAdmin)
#admin.site.register(ValueSource)
admin.site.register(DatePeriod)
admin.site.register(TimePeriod)
admin.site.register(TimeValue)
