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
TOPIC_BASE= f"app/{ROBOT_ID}"
TOPIC_CMD = f"{TOPIC_BASE}/cmd/#"
TOPIC_ONLINE = f"{TOPIC_BASE}/status/online"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT 재접속 완료")
        client.publish(TOPIC_ONLINE, json.dumps({"online": True}), qos=1, retain=True)
        client.subscribe(TOPIC_CMD, qos=1)
    else:
        print(f"연결 실패 rc={rc}")
def on_disconnect(client, userdata, rc):
    print(f"MQTT 연결 해제 rc = {rc}")

def _robot_cmd_topic(subtopic: str) -> str:
    return f"{ROBOT_BASE}/cmd/{subtopic}"
def _robot_resp_topic(subtopic: str, req_id: str) -> str:
    return f"{ROBOT_BASE}/resp/{subtopic}/{req_id}"
def _app_resp_topic(subtopic: str, req_id: Optional[str]) -> str:
    return f"{TOPIC_BASE}/resp/{subtopic}/{req_id}" if req_id else f"{TOPIC_BASE}/resp/{subtopic}"

def on_cmd(client, userdata, msg):
    parts = msg.topic.split('/')
    
    if not subtopic:
        client.publish(_app_resp_topic("", req_id),
                       json.dumps({"ok": False, "error": {"code":"bad_topic", "message": msg.topic}}),
                        qos=1)
        
    subtopic = '/'.join(parts[3:]) if len(parts) >= 4 else ""

    try:
        data = json.loads(msg.payload.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"[{msg.topic}] JSON 파싱 실패: {e}")
        return

    req_id = data.get("req_id")
    args   = data.get("args", {})

    # 1) req_id 없으면 에러 반환 후 종료
    if not req_id:
        client.publish(_app_resp_topic(subtopic, None),
                       json.dumps({"ok": False, "error":{"code":"missing_req_id","message":"req_id required"}}),
                       qos=1)
        return

    # ACK to app
    client.publish(_app_resp_topic(subtopic, req_id),
                   json.dumps({"ok": True, "data": {"state": "accepted"}}), qos=1)

    # 2) app → robot 포워드
    client.publish(_robot_cmd_topic(subtopic), json.dumps(data), qos=1)

    robot_resp = _robot_resp_topic(subtopic, req_id)

    # 타임아웃
    timeout_sec = float(args.get("timeout", 0)) or 3.0
    timer_ref = {"t": None}
    
    def _cancel_timer():
        t = timer_ref.get("t")
        if t:
            t.cancel()
            timer_ref["t"] = None
    def _robot_resp_cb(c ,u ,m):
        try:
            payload = json.loads(m.payload.decode('utf-8'))
        except Exception:
            
            client.publish(_app_resp_topic(subtopic, req_id), m.payload, qos=1)
            _cancel_timer()
            client.message_callback_remove(robot_resp)
            client.unsubscribe(robot_resp)
            return 
        client.publish(_app_resp_topic(subtopic, req_id), json.dumps(payload), qos=1)
        
        state = (payload.get("data") or {}).get("state")
        ok = payload.get("ok", True)
        
        if state == "result" or not ok:
            _cancel_timer()
            client.message_callback_remove(robot_resp)
            client.unsubscribe(robot_resp)
    client.message_callback_add(robot_resp, _robot_resp_cb)
    client.subscribe(robot_resp, qos =1)
    
    def _on_timeout():
        try:
            client.message_callback_remove(robot_resp)
            client.unsubscribe(robot_resp)
        except Exception:
            pass
        client.publish(_app_resp_topic(subtopic, req_id),
                       json.dumps({"ok": False, "error":{"code":"timeout","message":"robot no response"}}),
                       qos=1)
    timer_ref["t"] = threading.Timer(timeout_sec, _on_timeout)
    timer_ref["t"].start()
    
def run_listener():
    cli = mqtt.Client(client_id=f"cmd_listener_{ROBOT_ID}", clean_session=False)
    cli.will_set(TOPIC_ONLINE, json.dumps({"online":False}), qos=1, retain=True)
    cli.on_connect = on_connect
    cli.on_disconnect = on_disconnect
    cli.on_message = on_cmd
    cli.reconnect_delay_set(min_delay=1, max_delay=30)
    cli.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    cli.loop_forever()
    
if __name__ == "__main__":
    run_listener()