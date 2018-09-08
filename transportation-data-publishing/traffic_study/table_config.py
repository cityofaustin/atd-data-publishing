tables = [
    {
        "table_name": "volume",
        "fields": [
            {
                "name": "traffic_study_count_id",
                "type": "PRIMARY KEY TEXT",
                "description": "",
            },
            {"name": "row_id", "type": "TEXT", "description": ""},
            {"name": "data_file", "type": "TEXT", "description": ""},
            {"name": "site_code", "type": "TEXT", "description": ""},
            {"name": "datetime", "type": "TIMESTAMP WITH TIME ZONE", "description": ""},
            {"name": "year", "type": "INTEGER", "description": ""},
            {"name": "month", "type": "INTEGER", "description": ""},
            {"name": "day_of_month", "type": "INTEGER", "description": ""},
            {"name": "day_of_week", "type": "INTEGER", "description": ""},
            {"name": "time", "type": "TEXT", "description": ""},
            {"name": "channel", "type": "TEXT", "description": ""},
            {"name": "count_channel", "type": "INTEGER", "description": ""},
            {"name": "count_total", "type": "INTEGER", "description": ""},
        ],
    }
]
