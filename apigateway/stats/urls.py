from django.conf.urls import url
from rest_framework.urlpatterns import format_suffix_patterns
from stats import views
from stats import internet
from stats import vr
from stats import vpc
from stats import vpn
from stats import qos
from stats import user


urlpatterns = [
    url(r'^summary/(?P<accountId>[^/]+)$', views.stats_summary),
    url(r'^summary/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', views.stats_summary),
    url(r'^vpn/summary/(?P<accountId>[^/]+)$', views.vpn_stats_summary),
    url(r'^vpn/summary/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', views.vpn_stats_summary),
    url(r'^internet/traffic/(?P<accountId>[^/]+)$', internet.traffic_by_account),
    url(r'^internet/traffic/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', internet.traffic_by_account),
    url(r'^internet/bandwidth/(?P<accountId>[^/]+)$', internet.bandwidth_by_account),
    url(r'^internet/bandwidth/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', internet.bandwidth_by_account),
    url(r'^internet/traffic/top5/group/(?P<accountId>[^/]+)$', internet.top5_group_traffic),
    url(r'^internet/traffic/top5/group/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', internet.top5_group_traffic),
    url(r'^internet/traffic/top5/user/(?P<accountId>[^/]+)$', internet.top5_user_traffic),
    url(r'^internet/traffic/top5/user/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', internet.top5_user_traffic),
    url(r'^vr/traffic/(?P<accountId>[^/]+)$', vr.traffic_by_account),
    url(r'^vr/traffic/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vr.traffic_by_account),
    url(r'^vpc/traffic/(?P<accountId>[^/]+)$', vpc.traffic_by_account),
    url(r'^vpc/traffic/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vpc.traffic_by_account),
    url(r'^vpc/agg/traffic/total/(?P<accountId>[^/]+)$', vpc.agg_traffic),
    url(r'^vpc/agg/traffic/total/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vpc.agg_traffic),
    url(r'^vpn/traffic/(?P<accountId>[^/]+)$', vpn.traffic_by_account),
    url(r'^vpn/traffic/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vpn.traffic_by_account),
    url(r'^vpn/agg/activeuser/group/(?P<accountId>[^/]+)$', vpn.agg_active_user_by_group),
    url(r'^vpn/agg/activeuser/group/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vpn.agg_active_user_by_group),
    url(r'^vpn/agg/traffic/group/(?P<accountId>[^/]+)$', vpn.agg_traffic_by_group),
    url(r'^vpn/agg/traffic/group/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vpn.agg_traffic_by_group),
    url(r'^vpn/agg/traffic/user/(?P<accountId>[^/]+)$', vpn.agg_traffic_by_user),
    url(r'^vpn/agg/traffic/user/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vpn.agg_traffic_by_user),
    url(r'^vpn/agg/traffic/total/(?P<accountId>[^/]+)$', vpn.agg_traffic),
    url(r'^vpn/agg/traffic/total/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', vpn.agg_traffic),
    url(r'^vpn/session/current/(?P<accountId>[^/]+)$', vpn.session_current),
    url(r'^qos/(?P<accountId>[^/]+)$', qos.traffic_by_account),
    url(r'^qos/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', qos.traffic_by_account),
    url(r'^user/group/(?P<accountId>[^/]+)$', user.active_user_by_group),
    url(r'^user/group/(?P<accountId>[^/]+)/(?P<interval>[0-9]+)$', user.active_user_by_group),
]

urlpatterns = format_suffix_patterns(urlpatterns)