
Templates = {
"association_query":
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

"field_buckets":
{"script": {
	"lang": "mustache",
	"source": "{ \"aggs\": { \"field_buckets\": { \"range\": { \"field\" : \"{{field}}\", \"ranges\" : {{#toJson}}ranges{{/toJson}} }} }, \"size\": 0 }"
}},

"field_stats":
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

"filtered_range_query":
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

"range_buckets":
{"script": {
	"lang": "mustache",
	"source": "{ \"aggs\": { \"field_buckets\": { \"range\": { \"field\" : \"{{field}}\", \"ranges\" : {{#toJson}}ranges{{/toJson}} }} }, \"size\": 0, \"query\": { \"bool\" : { \"must\" : [{ \"range\": { \"_locrange\" : { \"gte\": \"{{start}}\", \"lt\": \"{{end}}\" } } }, { \"term\": {\"srcfeature\": \"{{srcfeature}}\"} }] } } }"
}},

"range_query":
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

"range_stats":
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

"sorted_range_query":
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
