docker run -d --name mqtt-broker \
 -p 1883:1883 -p 9001:9001 \
 -v ~/Desktop/HearoMQTT/config:/mosquitto/config \
 -v ~/Desktop/HearoMQTT/data:/mosquitto/data \
 -v ~/Desktop/HearoMQTT/logs:/mosquitto/log \
 -v ~/Desktop/HearoMQTT:/root/HearoMQTT \
 eclipse-mosquitto:2.0