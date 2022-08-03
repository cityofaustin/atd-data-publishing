PGREST_PUB = {
    "traffic_reports": {
        "pgrest_base_url": "https://atd-postgrest.austinmobility.io/legacy-scripts/traffic_reports",
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
    }
}

TRAFFIC_REPORT_SCRAPER = {
    "feed_url": "http://www.ci.austin.tx.us/qact/qact_rss.cfm",
    "endpoint": "https://atd-postgrest.austinmobility.io/legacy-scripts/traffic_reports",
    "primary_key": "traffic_report_id",
    "status_field": "traffic_report_status",
    "date_field": "published_date",
    "status_date_field": "traffic_report_status_date_time",
}
