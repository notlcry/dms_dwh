from django.conf.urls import url
from rest_framework.urlpatterns import format_suffix_patterns
from fault import views

urlpatterns = [
    url(r'^service/(?P<accountId>[^/]+)$', views.fault_summary),
]

urlpatterns = format_suffix_patterns(urlpatterns)