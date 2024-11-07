from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

def get_current_month_range():
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_date = first_day_of_current_month.isoformat() + "Z"
    end_date = today.isoformat() + "Z"
    
    return start_date, end_date

def get_last_month_range():
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    
    start_date = first_day_of_last_month.isoformat() + "Z"
    end_date = last_day_of_last_month.isoformat() + "Z"
    
    return start_date, end_date

def get_month_name(date_str):
    date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return date_obj.strftime("%B")

start_date, end_date = get_current_month_range()
month = get_month_name(start_date)

elk_url = os.getenv('ELK_CONNECTION_STRING')
elk_username = os.getenv('ELK_USERNAME')
elk_password = os.getenv('ELK_PASSWORD')
smtp_sender = os.getenv('SMTP_USERNAME')
smtp_password = os.getenv('SMTP_PASSWORD')

recipients = [
    "abhijit.bhadoria@bajajfinserv.in",
    "aniket.hazra@bajajfinserv.in",
    "tushar.sharma4@bajajfinserv.in",
    "vimal.sakkthi@bajajfinserv.in"
]

body = {
    "size": 0,
    "query": {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": [
                            {
                                "match_phrase": {
                                    "service.name": "drx-listing"
                                }
                            },
                            {
                                "match_phrase": {
                                    "service.name": "master_dropdown_prod"
                                }
                            }
                        ]
                    }
                }
            ],
            "should": [],
            "must_not": [
                {
                    "match_phrase": {
                        "transaction.name": "OperationHandler#handle"
                    }
                },
                {
                    "match_phrase": {
                        "processor.name": "metric"
                    }
                }
            ],
            "must": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": start_date,
                            "lte": end_date,
                            "format": "strict_date_optional_time"
                        }
                    }
                }
            ]
        }
    },
    "aggs": {
        "pivot": {
        "terms": {
            "field": "transaction.name",
            "size": "10000",
            "order": {
            "_count": "desc"
            }
        },
        "aggs": {
            "2a589d32-36c7-47f8-8091-7f76a62988a8": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "465d01d7-90cc-4b1a-9cb4-be858e87d5d6": {
                "filter": {
                    "exists": {
                    "field": "service.name"
                    }
                },
                "aggs": {
                    "docs": {
                    "top_hits": {
                        "size": 1,
                        "fields": [
                        "service.name"
                        ]
                    }
                    }
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "2a589d32-36c7-47f8-8091-7f76a62988a8"
            }
            },
            "7c02e560-793b-11ee-87c7-d761745a3d39": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "7c02e561-793b-11ee-87c7-d761745a3d39": {
                "filter": {
                    "exists": {
                    "field": "http.request.headers.Source"
                    }
                },
                "aggs": {
                    "docs": {
                    "top_hits": {
                        "size": 1,
                        "fields": [
                        "http.request.headers.Source"
                        ]
                    }
                    }
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "7c02e560-793b-11ee-87c7-d761745a3d39"
            }
            },
            "3128a510-793c-11ee-87c7-d761745a3d39": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "3128cc20-793c-11ee-87c7-d761745a3d39": {
                "bucket_script": {
                    "buckets_path": {
                    "count": "_count"
                    },
                    "script": {
                    "source": "count * 1",
                    "lang": "expression"
                    },
                    "gap_policy": "skip"
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "3128a510-793c-11ee-87c7-d761745a3d39"
            }
            },
            "4f1db650-793c-11ee-87c7-d761745a3d39": {
            "filter": {
                "bool": {
                "must": [],
                "filter": [
                    {
                    "bool": {
                        "filter": [
                        {
                            "bool": {
                            "should": [
                                {
                                "range": {
                                    "http.response.status_code": {
                                    "gte": "200"
                                    }
                                }
                                }
                            ],
                            "minimum_should_match": 1
                            }
                        },
                        {
                            "bool": {
                            "should": [
                                {
                                "range": {
                                    "http.response.status_code": {
                                    "lt": "400"
                                    }
                                }
                                }
                            ],
                            "minimum_should_match": 1
                            }
                        }
                        ]
                    }
                    }
                ],
                "should": [],
                "must_not": []
                }
            },
            "aggs": {
                "timeseries": {
                "auto_date_histogram": {
                    "field": "@timestamp",
                    "buckets": 1
                },
                "aggs": {
                    "4f1db651-793c-11ee-87c7-d761745a3d39": {
                    "bucket_script": {
                        "buckets_path": {
                        "count": "_count"
                        },
                        "script": {
                        "source": "count * 1",
                        "lang": "expression"
                        },
                        "gap_policy": "skip"
                    }
                    }
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms"
            }
            },
            "16fc97d0-793e-11ee-87c7-d761745a3d39": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "16fc97d1-793e-11ee-87c7-d761745a3d39-numerator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [
                        {
                        "bool": {
                            "filter": [
                            {
                                "bool": {
                                "should": [
                                    {
                                    "range": {
                                        "http.response.status_code": {
                                        "gte": "200"
                                        }
                                    }
                                    }
                                ],
                                "minimum_should_match": 1
                                }
                            },
                            {
                                "bool": {
                                "should": [
                                    {
                                    "range": {
                                        "http.response.status_code": {
                                        "lt": "400"
                                        }
                                    }
                                    }
                                ],
                                "minimum_should_match": 1
                                }
                            }
                            ]
                        }
                        }
                    ],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "16fc97d1-793e-11ee-87c7-d761745a3d39-denominator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [
                        {
                        "bool": {
                            "should": [
                            {
                                "exists": {
                                "field": "http.response.status_code"
                                }
                            }
                            ],
                            "minimum_should_match": 1
                        }
                        }
                    ],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "16fc97d1-793e-11ee-87c7-d761745a3d39": {
                "bucket_script": {
                    "buckets_path": {
                    "numerator": "16fc97d1-793e-11ee-87c7-d761745a3d39-numerator>_count",
                    "denominator": "16fc97d1-793e-11ee-87c7-d761745a3d39-denominator>_count"
                    },
                    "script": "params.numerator != null && params.denominator != null && params.denominator > 0 ? params.numerator / params.denominator : 0"
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "16fc97d0-793e-11ee-87c7-d761745a3d39"
            }
            },
            "6c85f0e0-793c-11ee-87c7-d761745a3d39": {
            "filter": {
                "bool": {
                "must": [],
                "filter": [
                    {
                    "bool": {
                        "filter": [
                        {
                            "bool": {
                            "should": [
                                {
                                "range": {
                                    "http.response.status_code": {
                                    "gte": "400"
                                    }
                                }
                                }
                            ],
                            "minimum_should_match": 1
                            }
                        },
                        {
                            "bool": {
                            "should": [
                                {
                                "range": {
                                    "http.response.status_code": {
                                    "lt": "500"
                                    }
                                }
                                }
                            ],
                            "minimum_should_match": 1
                            }
                        }
                        ]
                    }
                    }
                ],
                "should": [],
                "must_not": []
                }
            },
            "aggs": {
                "timeseries": {
                "auto_date_histogram": {
                    "field": "@timestamp",
                    "buckets": 1
                },
                "aggs": {
                    "6c85f0e1-793c-11ee-87c7-d761745a3d39": {
                    "bucket_script": {
                        "buckets_path": {
                        "count": "_count"
                        },
                        "script": {
                        "source": "count * 1",
                        "lang": "expression"
                        },
                        "gap_policy": "skip"
                    }
                    }
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms"
            }
            },
            "cc525310-793c-11ee-87c7-d761745a3d39": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "cc525311-793c-11ee-87c7-d761745a3d39-numerator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [
                        {
                        "bool": {
                            "filter": [
                            {
                                "bool": {
                                "should": [
                                    {
                                    "range": {
                                        "http.response.status_code": {
                                        "gte": "400"
                                        }
                                    }
                                    }
                                ],
                                "minimum_should_match": 1
                                }
                            },
                            {
                                "bool": {
                                "should": [
                                    {
                                    "range": {
                                        "http.response.status_code": {
                                        "lt": "500"
                                        }
                                    }
                                    }
                                ],
                                "minimum_should_match": 1
                                }
                            }
                            ]
                        }
                        }
                    ],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "cc525311-793c-11ee-87c7-d761745a3d39-denominator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [
                        {
                        "bool": {
                            "should": [
                            {
                                "exists": {
                                "field": "http.response.status_code"
                                }
                            }
                            ],
                            "minimum_should_match": 1
                        }
                        }
                    ],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "cc525311-793c-11ee-87c7-d761745a3d39": {
                "bucket_script": {
                    "buckets_path": {
                    "numerator": "cc525311-793c-11ee-87c7-d761745a3d39-numerator>_count",
                    "denominator": "cc525311-793c-11ee-87c7-d761745a3d39-denominator>_count"
                    },
                    "script": "params.numerator != null && params.denominator != null && params.denominator > 0 ? params.numerator / params.denominator : 0"
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "cc525310-793c-11ee-87c7-d761745a3d39"
            }
            },
            "386c9470-793d-11ee-87c7-d761745a3d39": {
            "filter": {
                "bool": {
                "must": [],
                "filter": [
                    {
                    "bool": {
                        "should": [
                        {
                            "range": {
                            "http.response.status_code": {
                                "gte": "500"
                            }
                            }
                        }
                        ],
                        "minimum_should_match": 1
                    }
                    }
                ],
                "should": [],
                "must_not": []
                }
            },
            "aggs": {
                "timeseries": {
                "auto_date_histogram": {
                    "field": "@timestamp",
                    "buckets": 1
                },
                "aggs": {
                    "386c9471-793d-11ee-87c7-d761745a3d39": {
                    "bucket_script": {
                        "buckets_path": {
                        "count": "_count"
                        },
                        "script": {
                        "source": "count * 1",
                        "lang": "expression"
                        },
                        "gap_policy": "skip"
                    }
                    }
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms"
            }
            },
            "753f1b20-793d-11ee-87c7-d761745a3d39": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "753f1b21-793d-11ee-87c7-d761745a3d39-numerator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [
                        {
                        "bool": {
                            "should": [
                            {
                                "range": {
                                "http.response.status_code": {
                                    "gte": "500"
                                }
                                }
                            }
                            ],
                            "minimum_should_match": 1
                        }
                        }
                    ],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "753f1b21-793d-11ee-87c7-d761745a3d39-denominator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [
                        {
                        "bool": {
                            "should": [
                            {
                                "exists": {
                                "field": "http.response.status_code"
                                }
                            }
                            ],
                            "minimum_should_match": 1
                        }
                        }
                    ],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "753f1b21-793d-11ee-87c7-d761745a3d39": {
                "bucket_script": {
                    "buckets_path": {
                    "numerator": "753f1b21-793d-11ee-87c7-d761745a3d39-numerator>_count",
                    "denominator": "753f1b21-793d-11ee-87c7-d761745a3d39-denominator>_count"
                    },
                    "script": "params.numerator != null && params.denominator != null && params.denominator > 0 ? params.numerator / params.denominator : 0"
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "753f1b20-793d-11ee-87c7-d761745a3d39"
            }
            },
            "e4e5aaf0-c0e3-11ee-a0e1-bfebc156e073": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "e4e5aaf1-c0e3-11ee-a0e1-bfebc156e073": {
                "percentiles": {
                    "field": "transaction.duration.us",
                    "percents": [
                    90
                    ],
                    "hdr": {
                    "number_of_significant_value_digits": 2
                    }
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "e4e5aaf0-c0e3-11ee-a0e1-bfebc156e073"
            }
            },
            "c85a3360-da1f-11ee-961e-797e4d320638": {
            "filter": {
                "bool": {
                "must": [],
                "filter": [
                    {
                    "bool": {
                        "should": [
                        {
                            "range": {
                            "http.response.status_code": {
                                "gt": "399"
                            }
                            }
                        }
                        ],
                        "minimum_should_match": 1
                    }
                    }
                ],
                "should": [],
                "must_not": []
                }
            },
            "aggs": {
                "timeseries": {
                "auto_date_histogram": {
                    "field": "@timestamp",
                    "buckets": 1
                },
                "aggs": {
                    "c85a3361-da1f-11ee-961e-797e4d320638": {
                    "bucket_script": {
                        "buckets_path": {
                        "count": "_count"
                        },
                        "script": {
                        "source": "count * 1",
                        "lang": "expression"
                        },
                        "gap_policy": "skip"
                    }
                    }
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms"
            }
            },
            "008503f0-da20-11ee-961e-797e4d320638": {
            "auto_date_histogram": {
                "field": "@timestamp",
                "buckets": 1
            },
            "aggs": {
                "008503f1-da20-11ee-961e-797e4d320638-numerator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [
                        {
                        "bool": {
                            "should": [
                            {
                                "range": {
                                "http.response.status_code": {
                                    "gt": "399"
                                }
                                }
                            }
                            ],
                            "minimum_should_match": 1
                        }
                        }
                    ],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "008503f1-da20-11ee-961e-797e4d320638-denominator": {
                "filter": {
                    "bool": {
                    "must": [],
                    "filter": [],
                    "should": [],
                    "must_not": []
                    }
                }
                },
                "008503f1-da20-11ee-961e-797e4d320638": {
                "bucket_script": {
                    "buckets_path": {
                    "numerator": "008503f1-da20-11ee-961e-797e4d320638-numerator>_count",
                    "denominator": "008503f1-da20-11ee-961e-797e4d320638-denominator>_count"
                    },
                    "script": "params.numerator != null && params.denominator != null && params.denominator > 0 ? params.numerator / params.denominator : 0"
                }
                }
            },
            "meta": {
                "timeField": "@timestamp",
                "index": "a670d8e0-14df-11ee-b8ca-23bcbd6928f0",
                "panelId": "0c4aeb3e-dd10-4e08-a75d-f91982675364",
                "intervalString": "2592000000ms",
                "seriesId": "008503f0-da20-11ee-961e-797e4d320638"
            }
            }
        }
        }
    },
}