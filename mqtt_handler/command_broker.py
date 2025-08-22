import paho.mqtt.client as mqtt
import json 
import os
from typing import Optional
import threading
import time


BROKER_HOST = os.getenv("BROKER_HOST", "broker")
BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
ROBOT_ID = os.getenv("ROBOT_ID", "robot001")

ROBOT_BASE = f"robot/{ROBOT_ID}"
APP_BASE= f"app/{ROBOT_ID}"

TOPIC_CMD = f"{APP_BASE}/cmd/#"
ROBOT_RESPONSE = f"{ROBOT_BASE}/resp/#"
TOPIC_ONLINE = f"{APP_BASE}/status/online"

COMMAND_HANDLER = f"command_handler/status"


def robot_cmd_topic(subtopic: str) -> str:
    return f"{ROBOT_BASE}/cmd/{subtopic}" if subtopic else f"{ROBOT_BASE}/cmd"
def robot_resp_topic(subtopic: str) -> str:
    return f"{ROBOT_BASE}/resp/{subtopic}" if subtopic else f"{ROBOT_BASE}/resp"
def app_resp_topic(subtopic: str) -> str:
    return f"{APP_BASE}/resp/{subtopic}" if subtopic else f"{APP_BASE}/resp"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT 연결 성공, 구독/상태 갱신 수행")
        client.publish(COMMAND_HANDLER, json.dumps({"online": True}), qos=1, retain=True)
        client.subscribe(TOPIC_CMD, qos=1)
        client.subscribe(ROBOT_RESPONSE, qos=1)
        client.message_callback_add(TOPIC_CMD, on_cmd)
        client.message_callback_add(ROBOT_RESPONSE, on_robot_response)
    else:
        print(f"연결 실패 rc={rc}")
        
def on_disconnect(client, userdata, rc):
    print(f"MQTT 연결 해제 rc = {rc}")


def extract_subtopic(kind: str, topic: str) -> str:
    
    parts = topic.split("/")
    if kind in parts:
        idx = parts.index(kind)
        return '/'.join(parts[idx+1:])
    return ""

def on_cmd(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    
    subtopic = extract_subtopic("cmd", msg.topic)
    
    if subtopic == "":
        err = {"ok": False, "error": {"code": "bad_topic", "message": f"need subtopic: {msg.topic}"}}
        client.publish(app_resp_topic(subtopic), json.dumps(err), qos=1)
        return
        
    try:
        data = json.loads(msg.payload.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"[{msg.topic}] JSON 파싱 실패: {e}")
        err = {"ok":False, "error": {"code": "bad_json", "message": str(e)}}
        client.publish(app_resp_topic(subtopic), json.dumps(err), qos=1)
        return

    req_id = data.get("req_id")
    
    if not req_id:
        err = {"ok": False, "error": {"code": "missing_req_id", "message": "req_id required"}}
        client.publish(app_resp_topic(subtopic), json.dumps(err), qos=1)
        return

    ack = {"req_id": req_id, "ok": True, "response": {"state": "accepted"}}
    client.publish(app_resp_topic(subtopic), json.dumps(ack), qos=1)

    client.publish(robot_cmd_topic(subtopic), json.dumps(data), qos=1)

def on_robot_response(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    subtopic = extract_subtopic("resp", msg.topic)
    
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        client.publish(app_resp_topic(subtopic), json.dumps(payload, separators=(',',':')), qos=1)
    except Exception:
        client.publish(app_resp_topic(subtopic), msg.payload, qos=1)
    
    
def run_listener():
    cli = mqtt.Client(client_id=f"cmd_listener_{ROBOT_ID}", clean_session=False)
    cli.will_set(TOPIC_ONLINE, json.dumps({"online":False}), qos=1, retain=True)
    cli.on_connect = on_connect
    cli.on_disconnect = on_disconnect
    cli.reconnect_delay_set(min_delay=1, max_delay=30)
    cli.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    cli.loop_forever()
    
if __name__ == "__main__":
    run_listener()