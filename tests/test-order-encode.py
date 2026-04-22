import sys, json, msgpack
sys.path.append("./")

from core.receiver import StreamReceiver

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████  Environment variables  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):

    sample = "tests/sample_order_update_WS.json"
    with open(sample, "r") as f:
        json_str_sample = f.read()
        json_sample = json.loads(json_str_sample)

    parsed = StreamReceiver.parse_message_ws(
        json_str_sample, "MySuperProfitableStrategy")
    
    encoded = msgpack.packb(parsed)
    decoded = msgpack.unpackb(encoded)
    action = decoded[0]

    keys = StreamReceiver.MSG_ZMQ_FORMAT_TYPES[action]
    json_decoded = dict(zip(keys, decoded))

    json_str_sample = json.dumps(indent = 4, obj = json_sample)
    json_str_decoded = json.dumps(indent = 4, obj = json_decoded)
    print("Sample (json):", json_str_sample)
    print("Parsed:", parsed)
    print("Encoded:", encoded)
    print("Size:", len(encoded))
    print("Decoded:", decoded)
    print("Decoded (json):", json_str_decoded)