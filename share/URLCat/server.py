import prova
import time
from flask import Flask
from flask import request
from flask import jsonify

prova.init()
app = Flask(__name__)

@app.route('/')
def summary():
    # http://10.0.75.1:5000/?x0=0&x1=0&y0=1&y1=1
    bottomright0 = float(request.args.get('x0'))
    bottomright1 = float(request.args.get('x1'))
    topright0 = float(request.args.get('y0'))
    topright1 = float(request.args.get('y1'))
    if (1):
        start = time.time()
        d = prova.compute((bottomright0, bottomright1), (topright0, topright1))
        return jsonify({"main":d["main"], "topics":d["topics"], "duration": (time.time() - start)})
    else:
        return "Bad request"

if __name__ == "__main__":
    app.run(host= '0.0.0.0')
