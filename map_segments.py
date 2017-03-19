#!/usr/bin/python3

import json
import time
import logging
logging.basicConfig(level=logging.INFO)

import geojson
import overpass
api = overpass.API()

_SEGMENTS_QUERY = "node(around:{},{},{})[rcn_ref={}];"

junctions_osm = geojson.FeatureCollection([])

for junction in junctions_wt["results"]["bindings"]:
    perimeter = 10.0
    q = _SEGMENTS_QUERY.format(perimeter,
                           junction["lat"]["value"],
                           junction["long"]["value"],
                           junction["nummer"]["value"])
    logging.debug(q)
    try:
        time.sleep(1)
        response = api.Get(q)
        print(response)
        logging.info("found {} osm equivalents for {}".format(len(response["features"]),
                                                              junction["junction"]["value"]))
        for feature in response["features"]:
            feature["properties"]["uri"] = junction["junction"]["value"]
        junctions_osm["features"].extend(response["features"])
    except overpass.OverpassError as e:
        logging.warning("Something went wrong querying overpass:", e)
        continue


insert_query_form = """
INSERT DATA {{
    GRAPH <{0}> {{
        <{1}> <{2}> {3}{4}.
    }}
}}
"""

select_query = os.getenv('URL_QUERY')
# select_query = select_query_form.format(os.getenv("MU_APPLICATION_GRAPH"),
#     os.getenv("SITE_PREDICATE"))

try:
    results = helpers.query(select_query)["results"]["bindings"]
except Exception as e:
    helpers.log("Querying SPARQL-endpoint failed:\n{}".format(e))

for result in results:
    try:
        url = result["url"]["value"]
    except KeyError as e:
        helpers.log('SPARQL query must contain "?url"')
    # if url in urls: #check if url already has scraped text in store
    #     continue
    try:
        helpers.log("Getting URL \"{}\"".format(url))
        doc_before = scrape(url)
        if  not doc_before: continue
        doc_lang = get_lang(doc_before)
        doc_after = cleanup(doc_before)
        insert_query = insert_query_form.format(os.getenv('MU_APPLICATION_GRAPH'),
            url, os.getenv('CONTENT_PREDICATE'),
            escape_helpers.sparql_escape(doc_after),
            '@'+doc_lang if doc_lang else '')
        try:
            helpers.update(insert_query)
        except Exception as e:
            helpers.log("Querying SPARQL-endpoint failed:\n{}".format(e))
            continue
    except Exception as e:
        helpers.log("Something went wrong ...\n{}".format(str(e)))
        continue
