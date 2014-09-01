from django.contrib.auth.decorators import login_required
from django.shortcuts import render_to_response
from django.template import RequestContext

@login_required
def index(req, **kwargs):
    return render_to_response('ct_timeseries/index.html', {}, context_instance=RequestContext(req))
