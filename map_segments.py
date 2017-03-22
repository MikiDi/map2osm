#!/usr/bin/python3

import xml.etree.ElementTree as ET
import time
import pprint

import geojson
import overpy

import helpers, escape_helpers

api = overpy.Overpass()

_SEGMENTS_SPARQL_QUERY = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ost: <http://w3id.org/ost/ns#> #Open Standard for Tourism Ecosystems Data
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX schema: <http://schema.org/>
PREFIX gr:<http://purl.org/goodrelations/v1#> #GoodRelations
PREFIX skos:<http://www.w3.org/2004/02/skos/core#> #SKOS Simple Knowledge Organization System
PREFIX adms:<http://www.w3.org/ns/adms#> #Asset Description Metadata Schema
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ((xsd:integer(STRAFTER(STR(?elem1), "List/"))) as ?n) ?elem1 ?junction1 ?junction2 ?connection ?junctionnr1 ?junctionnr2 ?lat ?long  WHERE {
	{
		select  ?route where {
			?route a ost:NetworkBasedRoute .
		}
		LIMIT 1
	}

	#BIND
	?route ost:trajectoryDescription ?desc. FILTER (STRENDS(str(?desc),"-reverse") = false)
	?desc rdf:rest* ?elem1.
	?elem1 rdf:first ?junction1.
	?elem1 rdf:rest ?elem2.
	?elem2 rdf:first ?junction2.
	OPTIONAL{
    	?connection ost:startJunction ?junction1.
    	?connection ost:endJunction ?junction2.
 	}
	?junction1 ost:belongsTo ?net.
	#?net ost:recreationType ost:RECTCYCL. FILTER (NOT EXISTS {?net ost:recreationType ost:RECTWALK.}) #filter not working?
	#?net gr:name "FNW 2.0"^^xsd:string.
	?junction1 ost:number ?junctionnr1.
	?junction1 <http://www.w3.org/ns/locn#location>/<http://www.w3.org/ns/locn#geometry> ?l.
	?l <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat;
		<http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long.
    ?junction2 ost:number ?junctionnr2.
}
ORDER BY ASC(?n)
"""

# junctions_osm = geojson.FeatureCollection([])
#
# for junction in junctions_wt["results"]["bindings"]:
#     perimeter = 10.0
#     q = _SEGMENTS_QUERY.format(perimeter,
#                            junction["lat"]["value"],
#                            junction["long"]["value"],
#                            junction["nummer"]["value"])


_INSERT_SPARQL_QUERY = """
INSERT DATA {{
    GRAPH <{0}> {{
        <{1}> <{2}> {3}{4}.
    }}
}}
"""
_SEGMENTS_QUERY = """
    relation
    ["type"="route"]
    ["route"="bicycle"]
    ["network"="rcn"]
    ["note"="{0}-{1}"]
    (around:500.0, {2}, {3}) -> .traject_{4};
    node["rcn_ref"="{0}"](> .traject_{4});
    node["rcn_ref"="{1}"](> .traject_{4});"""


# def query_segment(start, end, lat, lng, n):

def run():
    try:
        results = helpers.query(_SEGMENTS_SPARQL_QUERY)["results"]["bindings"]
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed:\n{}".format(e))

    #Assemble overpass query
    query = "[out:xml][timeout:400];\n("
    for i in range(len(results)):
        if i == 3: break
        if results[i]["junctionnr1"]["value"] != results[i]["junctionnr2"]["value"]:
            query += _SEGMENTS_QUERY.format(min(results[i]["junctionnr1"]["value"], results[i]["junctionnr2"]["value"]),
                                            max(results[i]["junctionnr1"]["value"], results[i]["junctionnr2"]["value"]),
                                            results[i]["lat"]["value"],
                                            results[i]["long"]["value"],
                                            i)
    query += ");\nout body;"
    #Run query
    try:
        helpers.log("Sending query: \n{}".format(query))
        response = api.query(query)
        helpers.log("Done querying")
    except Exception as e:
        helpers.log("Something went wrong while querying overpass...\n{}".format(str(e)))

    #Assemble overpass query
    query = "[out:xml][timeout:400];\n("
    for result in results :
        note = "{}-{}".format(result["junctionnr1"]["value"], result["junctionnr2"]["value"])
        try:
            segment = next((x for x in response.relations if x.tags["note"] == note), None)
        except KeyError:
            segment = None
        helpers.log("Rel for segment {}: {}".format(result["n"]["value"], segment))
    # root = ET.fromstring(country_data_as_string)
