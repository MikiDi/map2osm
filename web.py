from .map_segments import run

@app.route("/")
def exampleMethod():
    run()
    return "running ..."
