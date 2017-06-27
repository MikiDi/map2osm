#!/usr/bin/python3

import time
import pprint
import os
import copy
import json

import rdflib
import geojson
import overpy
# import overpass

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

SELECT DISTINCT ((xsd:integer(STRAFTER(STR(?elem1), "List/"))) as ?n) ?elem1 ?junction1 ?junction2 ?connection ?segment_uuid ?osmid ?junctionnr1 ?junctionnr2 ?lat ?long  WHERE {
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
	?segment <http://mu.semte.ch/vocabularies/ext/fietsroutes/voc/fromJunction> ?junction1.
	?segment <http://mu.semte.ch/vocabularies/ext/fietsroutes/voc/toJunction> ?junction2.
    ?segment <http://mu.semte.ch/vocabularies/ext/fietsroutes/voc/hasOSMid> ?osmid.
    ?segment <http://mu.semte.ch/vocabularies/core/uuid> ?segment_uuid.
    
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

_JSONAPI_SEGMENT = 	{
    "id": None,
    "type": "segments",
    "relationships": {},
    "attributes": {
        "n": None,
        "osmid": None,
        "geojson": None
    }
}

def run():
    try:
        results = helpers.query(_SEGMENTS_SPARQL_QUERY)["results"]["bindings"]
    except Exception as e:
        # FIXME returnjsonapi error obj
        helpers.log("Querying SPARQL-endpoint failed:\n{}".format(e))

    #Assemble overpass query
    query = "[out:json][timeout:400];\n("
    for result in results:
        try:
            query += "relation({});\n".format(result["osmid"]["value"])
        except KeyError:
            helpers.log("result n {} has no osm id".format(result["n"]["value"]))
            continue
    query += ");\n(._;>;);\nout body geom;"
    #Run overpass query
    try:
        helpers.log("Sending query: \n{}".format(query))
        segments = api.query(query)
        helpers.log("Done querying")
        helpers.log(str(len(segments.relations)))
        for rel in segments.relations:
            helpers.log(str(rel.id))
    except overpy.OverPyException as e:

        helpers.log("Something went wrong while querying overpass...\n{}".format(str(e)))
        return None

    # pprint.pprint(segments)
    # return json.dumps(segments)

    data = []
    for result in results:
        relation = next((x for x in segments.relations if x.id == int(result["osmid"]["value"])), None)
        if relation:
            helpers.log("Found matching relation for segment {}".format(result["osmid"]["value"]))
            linestring = []
            for way in relation.members:
                # helpers.log("out relationway")
                # helpers.log(str(type(way)))
                if type(way)==overpy.RelationWay:
                    helpers.log("In relationway")
                    for node in way.resolve().nodes:
                        helpers.log(str(node))
                        linestring.append((float(node.lon), float(node.lat))) # cast Decimal to float
                else:
                    continue
            segment = copy.deepcopy(_JSONAPI_SEGMENT)
            segment["id"] = result["segment_uuid"]["value"]
            segment["attributes"]["n"] = result["n"]["value"]
            segment["attributes"]["osmid"] = result["osmid"]["value"]
            segment["attributes"]["geojson"] = geojson.Feature(geometry=geojson.LineString(linestring))
            data.append(segment)
            # linestring = [  for way in relation.attributes for node in way.resolve().nodes ]
            # helpers.log()
            #multilinestring.append(linestring)
        else:
            helpers.log("Found NO matching relation for segment {}".format(result["osmid"]["value"]))
            continue
    return data
