#!/usr/bin/python3

import xml.etree.ElementTree as ET
import time
import pprint
import os

import rdflib
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
    FILTER NOT EXISTS{
    	?segment <http://mu.semte.ch/vocabularies/ext/fietsroutes/voc/fromJunction> ?junction1.
    	?segment <http://mu.semte.ch/vocabularies/ext/fietsroutes/voc/toJunction> ?junction2.
        ?segment <http://mu.semte.ch/vocabularies/ext/fietsroutes/voc/hasOSMid> ?osmid.
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

_INSERT_SPARQL_QUERY = """
INSERT DATA {{
    GRAPH <{0}> {{
        <{1}> <{2}> {3}{4}.
    }}
}}
"""

_SEGMENTS_OVERPASS_QUERY = """
    relation
    ["type"="route"]
    ["route"="bicycle"]
    ["network"="rcn"]
    ["note"="{0}-{1}"]
    (around:500.0, {2}, {3}) -> .traject_{4};
    node["rcn_ref"="{0}"](> .traject_{4});
    node["rcn_ref"="{1}"](> .traject_{4});"""

def build_insert_query(junction1, junction2, osm_segment_id):
    mu_uri = "http://mu.semte.ch/vocabularies/ext/fietsroutes/"
    voc_ns = rdflib.namespace.Namespace(mu_uri + "voc/")
    res_ns = rdflib.namespace.Namespace(mu_uri + "resources/")

    graph = rdflib.graph.Graph()
    junction1_uri = rdflib.term.URIRef(junction1)
    junction2_uri = rdflib.term.URIRef(junction2)
    segment_uuid = helpers.generate_uuid()
    segment_uri = rdflib.term.URIRef(mu_uri + "resources/" +
        "segment/" + str(segment_uuid))
    graph.add((segment_uri, # type MalletTopic
               rdflib.namespace.RDF["type"],
               voc_ns["segment"]))
    graph.add((segment_uri, # type MalletTopic
               rdflib.term.URIRef("http://mu.semte.ch/vocabularies/core/uuid"),
               rdflib.term.Literal(str(segment_uuid))))
    graph.add((segment_uri, # type MalletTopic
               voc_ns["fromJunction"],
               junction1_uri))
    graph.add((segment_uri,
               voc_ns["toJunction"],
               junction2_uri))
    graph.add((segment_uri,
               voc_ns["hasOSMid"],
               rdflib.term.Literal(osm_segment_id)))
    return """
    INSERT DATA {{
        GRAPH <{0}> {{
            {1}
        }}
    }}
    """.format(os.getenv('MU_APPLICATION_GRAPH'), graph.serialize(format='nt').decode('utf-8'))
# def query_segment(start, end, lat, lng, n):

def run():
    try:
        results = helpers.query(_SEGMENTS_SPARQL_QUERY)["results"]["bindings"]
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed:\n{}".format(e))

    #Assemble overpass query
    query = "[out:xml][timeout:400];\n("
    for i in range(len(results)):
        if results[i]["junctionnr1"]["value"] != results[i]["junctionnr2"]["value"]:
            query += _SEGMENTS_OVERPASS_QUERY.format(min(results[i]["junctionnr1"]["value"], results[i]["junctionnr2"]["value"]),
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

    #Map overpass query results back to our data
    for result in results :
        note = "{}-{}".format(min(result["junctionnr1"]["value"], result["junctionnr2"]["value"]),
                                        max(result["junctionnr1"]["value"], result["junctionnr2"]["value"]))
        try:
            segment = next((x for x in response.relations if x.tags["note"] == note), None)
            helpers.update(build_insert_query(result["junction1"]["value"], result["junction2"]["value"], segment.id))
            helpers.log("Rel for segment {} (NOTE={}): {}\n{}".format(result["n"]["value"], note, segment,
                        build_insert_query(result["junction1"]["value"], result["junction2"]["value"], segment.id)))
        except (KeyError, AttributeError) as e:
            helpers.log("Something went wrong mapping back result {} (NOTE={})".format(str(result), note))
            continue
    # root = ET.fromstring(country_data_as_string)
