HearoMQTT 구현

## 📁 프로젝트 구조 설명 – HEAROMQTT

```
HEAROMQTT/
├── mqtt_handler/         # MQTT 메시지 수신 및 처리 로직을 담당하는 모듈
├── config/               # 설정 파일을 모아두는 폴더
│   └── mosquitto.conf    # Mosquitto 브로커 설정 파일
├── data/                 # 브로커가 사용하는 메시지 persistence 데이터 저장 위치
|
├── logs/                 # 브로커 및 앱의 로그가 저장되는 디렉토리
│   └── mosquitto.log     # Mosquitto 로그 파일
│
├── cert/                 # SSL 인증서 관련 파일 저장 위치 (TLS 사용 시 필요)
│
├── .dist/                # 패키징된 실행 파일, 빌드 결과물 등을 저장 (필요 시)
│
├── Dockerfile            # MQTT 브로커 및 관련 코드 실행 환경을 정의한 Docker 설정 파일
│
├── main.py               # 서버 진입점, 실행 시 MQTT 브로커와 통신하고 처리를 시작함
│
├── requirements.txt      # Python 의존성 패키지 목록 (pip install -r requirements.txt)
│
└── README.md             # 프로젝트 소개, 설치 및 실행 방법, 구조 설명 등을 포함한 문서
```

---

### 💡 주의사항
- `mqtt_handler/`는 상황에 따라 ROS → MQTT 혹은 Web → MQTT 요청을 처리하는 중간 계층 역할도 포함할 수 있습니다.
- `Dockerfile`에서는 `config/`, `data/`, `logs/`, `cert/` 등을 적절히 마운트해야 합니다.
