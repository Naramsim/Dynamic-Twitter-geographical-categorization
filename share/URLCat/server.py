import categorize
import booter
import time
from flask import Flask
from flask import request
from flask import jsonify


booter.parse()
booter.init_redis("10.0.75.2")
booter.init_context()

categorize.build_grid()

app = Flask(__name__)

@app.route('/')
def summary():
    # http://10.0.75.1:5000/?x0=0&y0=0&x1=1&y1=1
    bottomleft_x = float(request.args.get('x0'))
    print(request.args.get('y0'))
    bottomleft_y = float(request.args.get('y0'))
    topright_x = float(request.args.get('x1'))
    topright_y = float(request.args.get('y1'))

    if bottomleft_x is not None or bottomleft_y is not None or topright_x is not None or topright_y is not None:
        start = time.time()
        result = categorize.compute_area((bottomleft_x, bottomleft_y), (topright_x, topright_y))
        end = time.time()

        if result:
            return jsonify({"main": result["main"], "topics": result["topics"], "duration": end-start})
        else:
            return jsonify({"main": None, "topics": None, "duration": start-end})
    else:
        return "Bad request"

if __name__ == "__main__":
    app.run(host="0.0.0.0")
