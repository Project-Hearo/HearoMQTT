import paho.mqtt.client as mqtt
import json
import os
BROKER_HOST = os.getenv("BROKER_HOST", "broker")
BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
ROBOT_ID = os.getenv("ROBOT_ID", "robot001")

TOPIC_ONLINE = f"robot/{ROBOT_ID}/status/online"

def on_connect(client, userdatam, flags, rc):
    if rc == 0:
        print("MQTT 브로커 연결 성공")
        client.subscribe(TOPIC_ONLINE)
        print(f"토픽 구독 시작: {TOPIC_ONLINE}")
    else:
        print(f"연결 실패 rc={rc}")
        
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        print(f"수신된 메시지: {data}")
        
        if data.get("online") is True:
            print(f"로봇 {ROBOT_ID} 온라인 상태")
        elif data.get("online") is False:
            print(f"로봇 {ROBOT_ID} 오프라인 상태")
        else:
            print(f"예상치 못한 데이터: {data}")
    except Exception as e:
        print(f"메시지 처리 오류: {e}")        
def run_listener():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_forever()
    
if __name__ == "__main__":
    run_listener()
    