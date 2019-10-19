PGREST_PUB = {
    "traffic_reports": {
        "pgrest_base_url": "http://transportation-data-01-58741847.us-east-1.elb.amazonaws.com/traffic_reports",
        "primary_key": "traffic_report_id",
        'limit' : 2000000,
        "modified_date_field": "traffic_report_status_date_time",
        "service_id": "444c8a2b4388485283c2968bd99ddf6c",
        "socrata_resource_id": "dx9v-zd7x",
        "location_fields": {
            "lat": "latitude",
            "lon": "longitude",
            "location_field": "location",
        },
        "date_fields" : [
            "traffic_report_status_date_time",
            "published_date",
        ]
    },
    "dockless_trips": {
        "pgrest_base_url": "http://transportation-data-01-58741847.us-east-1.elb.amazonaws.com/dockless_public",
        "primary_key": "trip_id",
        'limit' : 100000,
        "modified_date_field": "modified_date",
        "socrata_resource_id": "7d8e-dm7r",
        "date_fields" : [
            "start_time",
            "end_time",
            "modified_date"
        ]
    }
}

TRAFFIC_REPORT_SCRAPER = {
    "feed_url": "http://www.ci.austin.tx.us/qact/qact_rss.cfm",
    "endpoint": "http://transportation-data-01-58741847.us-east-1.elb.amazonaws.com/traffic_reports",
    "primary_key": "traffic_report_id",
    "status_field": "traffic_report_status",
    "date_field": "published_date",
    "status_date_field": "traffic_report_status_date_time",
}