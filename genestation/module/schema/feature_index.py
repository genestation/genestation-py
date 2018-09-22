FeatureIndexTemplate = {
	"index_patterns" : ["feature.*"],
	"template" : "feature.*",
	"mappings": { "doc": {
		"dynamic": False,
		"properties": {
			"genome": {"type": "keyword"},
			"ftype": {"type": "keyword"},
			"name": {"type": "keyword"},
			"dbxref": {"type": "keyword"},
			"region": {"type": "keyword"},
			"locrange": {"type": "long_range"},
			"child": { "properties": {
				"genome": {"type": "keyword"},
				"ftype": {"type": "keyword"},
				"name": {"type": "keyword"},
				"dbxref": {"type": "keyword"},
				"region": {"type": "keyword"},
				"locrange": {"type": "long_range"},
				"data": {"dynamic": True, "type": "object"},
			}},
			"association": { "properties": {
				"type": "nested",
				"genome": {"type": "keyword"},
				"ftype": {"type": "keyword"},
				"name": {"type": "keyword"},
				"dbxref": {"type": "keyword"},
				"region": {"type": "keyword"},
				"locrange": {"type": "long_range"},
				"data": {"dynamic": True, "type": "object"},
			}},
			"data": {"dynamic": True, "type": "object"},
			"variant": { "properties": {
				"ref": { "properties": {
					"base": { "type": "text" },
				}},
				"alt": {
					"type": "nested",
					"properties": {
						"base": { "type": "text" },
				}},
			}},
		}
	} }
}

FeatureSearchTemplate = {
"feature.association_query":
{"script": {
	"lang": "mustache",
	"source": {
		"query": {
			"bool" : {
				"should" : [{
					"nested":{"path": "data.gwas","query":{"term": {"data.gwas.mapped_gene.keyword":"{{gene}}"}}}
				}, {
					"nested":{"path": "data.gwas","query":{"term": {"data.gwas.reported_gene.keyword":"{{gene}}"}}}
				}, {
					"nested":{"path": "data.phewas","query":{"term": {"data.phewas.gene.keyword":"{{gene}}"}}}
				}]
			}
		}
	}
}},

"feature.field_buckets":
{"script": {
	"lang": "mustache",
	"source": "{ \"aggs\": { \"field_buckets\": { \"range\": { \"field\" : \"{{field}}\", \"ranges\" : {{#toJson}}ranges{{/toJson}} }} }, \"size\": 0 }"
}},

"feature.field_stats":
{"script": {
	"lang": "mustache",
	"source": {
		"aggs": {
			"field_stats": { "stats": { "field" : "{{field}}"}},
			"field_percentiles": { "percentiles": { "field" : "{{field}}"}}
		},
		"size": 0
	}
}},

"feature.filtered_range_query":
{"script": {
	"lang": "mustache",
	"source": {
		"_source": "{{field}}",
		"query": {
			"bool" : {
				"must" : [{
					"range": {
						"_locrange" : {
							"gte": "{{start}}",
							"lt": "{{end}}"
						}
					}
				}, {
					"term": {"srcfeature": "{{srcfeature}}"}
				}]
			}
		}
	}
}},

"feature.range_buckets":
{"script": {
	"lang": "mustache",
	"source": "{ \"aggs\": { \"field_buckets\": { \"range\": { \"field\" : \"{{field}}\", \"ranges\" : {{#toJson}}ranges{{/toJson}} }} }, \"size\": 0, \"query\": { \"bool\" : { \"must\" : [{ \"range\": { \"_locrange\" : { \"gte\": \"{{start}}\", \"lt\": \"{{end}}\" } } }, { \"term\": {\"srcfeature\": \"{{srcfeature}}\"} }] } } }"
}},

"feature.range_query":
{"script": {
	"lang": "mustache",
	"source": {
		"query": {
			"bool" : {
				"must" : [{
					"range": {
						"_locrange" : {
							"gte": "{{start}}",
							"lt": "{{end}}"
						}
					}
				}, {
					"term": {"srcfeature": "{{srcfeature}}"}
				}]
			}
		}
	}
}},

"feature.range_stats":
{"script": {
	"lang": "mustache",
	"source": {
		"aggs": {
			"field_stats": { "stats": { "field" : "{{field}}"}},
			"field_percentiles": { "percentiles": { "field" : "{{field}}"}}
		},
		"size": 0,
		"query": {
			"bool" : {
				"must" : [{
					"range": {
						"_locrange" : {
							"gte": "{{start}}",
							"lt": "{{end}}"
						}
					}
				}, {
					"term": {"srcfeature": "{{srcfeature}}"}
				}]
			}
		}
	}
}},

"feature.sorted_range_query":
{"script": {
	"lang": "mustache",
	"source": {
		"sort": [
			{ "{{field}}": {
				"order": "{{order}}",
				"mode": "{{mode}}"
			}}
		],
		"query": {
			"bool" : {
				"must" : [{
					"range": {
						"_locrange" : {
							"gte": "{{start}}",
							"lt": "{{end}}"
						}
					}
				}, {
					"term": {"srcfeature": "{{srcfeature}}"}
				}]
			}
		}
	}
}}
}
