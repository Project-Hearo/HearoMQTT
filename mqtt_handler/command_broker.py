import paho.mqtt.client as mqtt
import json 
import os
BROKER_HOST = os.getenv("BROKER_HOST", "broker")
BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
ROBOT_ID = os.getenv("ROBOT_ID", "robot001")

TOPIC_CMD = f"app/{ROBOT_ID}/cmd/#"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT 재접속 완료")
        client.subscribe(TOPIC_CMD, qos=1)
    else:
        print(f"연결 실패 rc={rc}")
        
def on_cmd(client, userdata, msg):
    parts = msg.topic.split('/')
    subtopic = '/'.join(parts[3:])
    
    try:
        data = json.loads(msg.payload.decode())
    except json.JSONDecodeError as e:
        print(f"[{msg.topic}] JSON 파싱 실패: {e}")
        return
    
    if subtopic == "slam":
        action = data.get("action")
        if action == "start":
            print("start신호를 받았슴미다!")
        elif action == "stop":
            print("stop신호를 받았슴미다!")
        else:
            print(f"[slam] 알 수 없는 action: {action}")
    else:
        print(f"[cmd] 지원 안 하는 subtopic: {subtopic}")
def run_listener():
    
    cli = mqtt.Client("cmd_listener")
    cli.subscribe(TOPIC_CMD, qos=1)
    cli.on_message = on_cmd
    cli.on_connect = on_connect
    cli.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    cli.loop_forever()
    
if __name__ == "__main__":
    run_listener()