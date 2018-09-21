MetaIndexTemplate = {
	"index_patterns" : ["stats.*"],
	"template" : "stats.*",
	"mappings": { "doc": {
		"dynamic": False,
		"properties": {
			"field": {"type": "keyword"},
			"description": {"type": "text"},
			"type": {"type": "keyword"},
			"stats": {
				"properties": {
					"count": {"type": "long"},
					"min": {"type": "double"},
					"max": {"type": "double"},
					"sum": {"type": "double"},
					"sum_of_squares": {"type": "double"},
					"variance": {"type": "double"},
					"std_deviation": {"type": "double"},
					"std_deviation_bounds": {
						"properties": {
							"upper": {"type": "double"},
							"lower": {"type": "double"},
						}
					},
					"percentiles": {
						"properties": {
							"1.0": {"type": "double"},
							"5.0": {"type": "double"},
							"25.0": {"type": "double"},
							"50.0": {"type": "double"},
							"75.0": {"type": "double"},
							"95.0": {"type": "double"},
							"99.0": {"type": "double"},
						}
					},
					"histogram": {
						"properties": {
							"key": {"type": "keyword"},
							"from": {"type": "double"},
							"to": {"type": "double"},
							"doc_count": {"type": "long"},
						}
					}
				}
			},
		}
	} }
}

MetaSearchTemplate = {
"meta.field_buckets": {
	"script": '{ "aggs": { "field_buckets": { "range": { "field" : "{{field}}", "ranges" : {{#toJson}}ranges{{/toJson}} }} }, "size": 0 }'
},

"meta.field_stats":
{"script": {
	"aggs": {
		"field_stats": { "extended_stats": { "field" : "{{field}}"}},
		"field_percentiles": { "percentiles": { "field" : "{{field}}"}}
	},
	"size": 0
}},
}
