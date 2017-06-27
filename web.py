import json


import helpers

from .map_segments import run
from .query_segments import run as run2

@app.route("/map2osm")
def exampleMethod():
    run()
    return "running ..."

@app.route("/fietsroutes/<fietsroute_id>/segments")
def exampleMethod2(fietsroute_id):
    try:
        data = run2()
        return json.dumps({"data": data})
    except Exception as e:
        helpers.log(str(e))
        return json.dumps({"errors": [
                          {
                            "status": "500",
                            "title":  "Server Error",
                            "detail": "An unexpected error came up."
                          }
                        ]})
