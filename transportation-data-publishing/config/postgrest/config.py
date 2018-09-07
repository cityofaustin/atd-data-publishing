PGREST_PUB = {
    "traffic_reports": {
        "base_url": "http://transportation-data.austintexas.io/traffic_reports",
        "primary_key": "traffic_report_id",
        "modified_date_field": "traffic_report_status_date_time",
        "service_id": "444c8a2b4388485283c2968bd99ddf6c",
        "socrata_resource_id": "dx9v-zd7x",
        "location_fields": {
            "lat": "latitude",
            "lon": "longitude",
            "location_field": "location",
        },
    }
}

TRAFFIC_REPORT_SCRAPER = {
    "feed_url": "http://www.ci.austin.tx.us/qact/qact_rss.cfm",
    "endpoint": "http://transportation-data.austintexas.io/traffic_reports",
    "primary_key": "traffic_report_id",
    "status_field": "traffic_report_status",
    "date_field": "published_date",
    "status_date_field": "traffic_report_status_date_time",
}