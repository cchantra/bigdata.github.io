
# Elastic Search Helper Example
(https://github.com/haribaskar/elasticsearch)

#### Find the data set here [./data](./data) 
# Cluster Health Check

## Overall Cluster Health
GET /_cat/health?v

## Node Health
GET /_cat/nodes?v

## List Indices
GET /_cat/indices?v

## Create 'sales' Index
PUT /sales

## Add 'order' to 'sales' index
PUT /sales/order/123
```
{
  "orderID":"123",
  "orderAmount":"500"
}
```
## Retrieve document
GET /sales/order/123


## Delete index
DELETE /sales

## List indices
GET /_cat/indices?v
# Create File with Requests (make sure to include new line at end of file)
```
vi reqs
{ "index" : { "_index" : "my-test", "_type" : "my-type", "_id" : "1" } }
{ "col1" : "val1"}
{ "index" : { "_index" : "my-test", "_type" : "my-type", "_id" : "2" } }
{ "col1" : "val2"}
{ "index" : { "_index" : "my-test", "_type" : "my-type", "_id" : "1" } }
{ "col1" : "val3" }
```
# Load from CURL
```
curl -s -H "Content-Type: application/x-ndjson" -XPOST localhost:9200/_bulk --data-binary "@reqs"; echo
```
# Check Kibana
GET /my-test
GET /my-test/my-type/1


# Load from Console
POST _bulk
```
{ "index" : { "_index" : "my-test-console", "_type" : "my-type", "_id" : "1" } }
{ "col1" : "val1" }
{ "index" : { "_index" : "my-test-console", "_type" : "my-type", "_id" : "2" } }
{ "col1" : "val2"}
{ "index" : { "_index" : "my-test-console", "_type" : "my-type", "_id" : "3" } }
{ "col1" : "val3" }
```

# Inspect data: Accounts.json
```
head accounts.json
```
# Load via curl, notice the endpoint and type
```
curl -H 'Content-Type: application/x-ndjson' -XPOST 'localhost:9200/bank/account/_bulk?pretty' --data-binary @accounts.json
```
# check inside ES
GET /_cat/indices
GET /bank


# Add mapping for lat/lon geo properties for logs
```
PUT /logstash-2015.05.18
{
  "mappings": {
    "log": {
      "properties": {
        "geo": {
          "properties": {
            "coordinates": {
              "type": "geo_point"
            }
          }
        }
      }
    }
  }
}
```
# Create two more to simulate daily logs
PUT /logstash-2015.05.19
```
{
  "mappings": {
    "log": {
      "properties": {
        "geo": {
          "properties": {
            "coordinates": {
              "type": "geo_point"
            }
          }
        }
      }
    }
  }
}
```
PUT /logstash-2015.05.20
```
{
  "mappings": {
    "log": {
      "properties": {
        "geo": {
          "properties": {
            "coordinates": {
              "type": "geo_point"
            }
          }
        }
      }
    }
  }
}
```
# Check out structure of log data
head logs.jsonl

# Import log files
```
curl -H 'Content-Type: application/x-ndjson' -XPOST 'localhost:9200/_bulk?pretty' --data-binary @logs.jsonl
```
# Check ES for data
GET /_cat/indices/logstash-*

# Change default index pattern in Kibana

#Load Shakespeare data: Check out shakespeare.json
head shakespeare.json

# Shakespeare Schema
```
{
    "line_id": INT,
    "play_name": "String",
    "speech_number": INT,
    "line_number": "String",
    "speaker": "String",
    "text_entry": "String",
}
```
# Create Shakespeare index with data types
```
PUT /shakespeare
{
 "mappings" : {
  "_default_" : {
   "properties" : {
    "speaker" : {"type": "keyword" },
    "play_name" : {"type": "keyword" },
    "line_id" : { "type" : "integer" },
    "speech_number" : { "type" : "integer" }
   }
  }
 }
}
```
# Load Shakespeare data
```
curl -H 'Content-Type: application/x-ndjson' -XPOST 'localhost:9200/shakespeare/_bulk?pretty' --data-binary @shakespeare.json
```
# Check out index in ES
GET /shakespeare
GET /_cat/indices?v


# show me everything
GET bank/account/_search

# find CA accounts only
```
GET bank/account/_search
{
  "query": {
    "match": {
      "state": "CA"
    }
  }
}
```
# find "Techade" accounts in CA only
```
GET bank/account/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": {"state": "CA"} },
        { "match": {"employer": "Techade"}}
      ]
    }
  }
}
```
# find non "Techade" accounts outside of CA
```
GET bank/account/_search
{
  "query": {
    "bool": {
      "must_not": [
        { "match": {"state": "CA"} },
        { "match": {"employer": "Techade"}}
      ]
    }
  }
}
```
# Boost results for Smith
```
GET bank/account/_search
{
  "query": {
    "bool": {
      "should": [
        { "match": {"state": "CA"} },
        { "match": {
          "lastname": {
            "query": "Smith",
            "boost": 3
            }
          }
        }
      ]
    }
  }
}
```

# Term Query
```
GET bank/account/_search
{
  "query": {
    "term": {
      "account_number": 516
    }
  }
}
```
# Returns null because "state" is a text field (hence not an exact match)
```
GET bank/account/_search
{
  "query": {
    "term": {
      "state": "RI"
    }
  }
}
```
# This works because it uses the "analysis" process
```
GET bank/account/_search
{
  "query": {
    "match": {
      "state": "RI"
    }
  }
}
```
# Terms can return multiple results
```
GET bank/account/_search
{
  "query": {
    "terms": {
      "account_number": [516,851]
    }
  }
}
```



# Range Queries
## gte = Greater-than or equal to
## gt = Greater-than
## lte = Less-than or equal to
## lt = Less-than

# Show all accounts between 516 and 851, boosting the importance
```
GET bank/account/_search
{
  "query": {
    "range": {
      "account_number": {
        "gte": 516,
        "lte": 851,
        "boost": 2
      }
    }
  }
}
```
# Show all account holders older than 35
```
GET bank/account/_search
{
  "query": {
    "range": {
      "age": {
        "gt": 35
      }
    }
  }
}
```
# Basic Example
```
GET bank/_analyze
{
  "tokenizer" : "standard",
  "text" : "The Moon is Made of Cheese Some Say"
}
```
# Mixed String
```
GET bank/_analyze
{
  "tokenizer" : "standard",
  "text" : "The Moon-is-Made of Cheese.Some Say$"
}
```
# Uset the letter tokenizer
```
GET bank/_analyze
{
  "tokenizer" : "letter",
  "text" : "The Moon-is-Made of Cheese.Some Say$"
}
```
# Standard tokenizer URL
```
GET bank/_analyze
{
  "tokenizer": "standard",
  "text": "you@example.com login at https://bensullins.com attempt"
}
```
GET bank/_analyze
```
{
  "tokenizer": "uax_url_email",
  "text": "you@example.com login at https://bensullins.com attempt"
}
```
# Where it breaks, two fields with diff analyzers
```
PUT /idx1
{
  "mappings": {
    "t1": {
      "properties": {
        "title": {
            "type": "text",
            "analyzer" : "standard"
        },
        "english_title": {
            "type":     "text",
            "analyzer": "english"
        }
      }
    }
  }
}
```
GET idx1

GET idx1/_analyze
```
{
  "field": "title",
  "text": "Bears"
}
```
GET idx1/_analyze
```
{
  "field": "english_title",
  "text": "Bears"
}
```

# Count of Accounts by State
# Must be keyword field
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
```
# Add average balance in each state
# Nesting the metric inside the agg
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      },
      "aggs": {
        "avg_bal": {
          "avg": {
            "field": "balance"
          }
        }
      }
    }
  }
}
```
# Breakdown further with Nesting
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      },
      "aggs": {
        "avg_bal": {
          "avg": {
            "field": "balance"
          }
        },
        "gender":{
          "terms": {
            "field": "gender.keyword"
          }
        }
      }
    }
  }
}
```
# Add avg_price metric to lowest level
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      },
      "aggs": {
        "avg_bal": {
          "avg": {
            "field": "balance"
          }
        },
        "gender":{
          "terms": {
            "field": "gender.keyword"
          },
          "aggs": {"avg_bal": {"avg": {"field": "balance"} }
          }
        }
      }
    }
  }
}
```

## Get stats about bank balances
## Size=1 to omit search results
```
GET bank/account/_search
{
  "size": 1,
  "aggs": {
    "balance-stats": {
      "stats": {
        "field": "balance"
      }
    }
  }
}
```

# Count of Accounts by State
# Must be keyword field
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
```
# This is the equivalent of using match_all
GET bank/account/_search
```
{
  "size": 0,
  "query": {
    "match_all": {}
  },
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
```
# Aggs work in the context of the query, so apply a filter like normal
```
GET bank/account/_search
{
  "size": 0,
  "query": {
    "match": {
      "state.keyword": "CA"
    }
  },
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
```
# Filter on terms
```
GET bank/account/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {"match": {"state.keyword": "CA"}},
        {"range": {"age": {"gt": 35}}}
      ]
    }
  },
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
```
# Lets add a metric back in
```
GET bank/account/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {"match": {"state.keyword": "CA"}},
        {"range": {"age": {"gt": 35}}}
      ]
    }
  },
  "aggs": {
    "states": {
      "terms": {
        "field": "state.keyword"
      },
      "aggs": {"avg_bal": {"avg": {"field": "balance"} }}
    }
  }
}
```
# Just filter the results
```
GET bank/account/_search
{
  "size": 0,
  "query": {
    "match": {"state.keyword": "CA"}
  },
  "aggs": {
    "over35":{
      "filter": {
        "range": {"age": {"gt": 35}}
      },
    "aggs": {"avg_bal": {"avg": {"field": "balance"} }}
    }
  }
}
```

# Look at state avg and global average
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "state_avg": {
      "terms": {
        "field": "state.keyword"
      },
      "aggs": {"avg_bal": {"avg": {"field": "balance"}}}
    },
    "global_avg": {
      "global": {},
      "aggs": {"avg_bal": {"avg": {"field": "balance"}}}
    }
  }
}
```

# Look at the percentiles for the balances
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "pct_balances": {
      "percentiles": {
        "field": "balance",
        "percents": [
          1,
          5,
          25,
          50,
          75,
          95,
          99
        ]
      }
    }
  }
}
```
# Calculate High Dynamic Range (HDR) Historgram
```
GET bank/account/_search
{
  "size": 0,
  "aggs": {
    "pct_balances": {
      "percentiles": {
        "field": "balance",
        "percents": [
          1,
          5,
          25,
          50,
          75,
          95,
          99
        ],
        "hdr": {
          "number_of_significant_value_digits": 3
        }
      }
    }
  }
}
```
# The percentile ranks agg for checking a individual values
GET bank/account/_search
```
{
  "size": 0,
  "aggs": {
    "bal_outlier": {
      "percentile_ranks": {
        "field": "balance",
        "values": [35000,50000],
        "hdr": {
          "number_of_significant_value_digits": 3
        }
      }
    }
  }
}
```
# Create a histogram
GET bank/account/_search
```
{
  "size": 0,
  "aggs": {
    "bals": {
      "histogram": {
        "field": "balance",
        "interval": 500
      }
    }
  }
}
```
