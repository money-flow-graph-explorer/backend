# Money Flow Graph Explorer — Backend

IBM AMLSim 거래 데이터를 Neo4j 그래프로 적재하고, 자금세탁 의심 패턴(순환·다단 경유·집중 송금·분산 송금)을
탐지하는 Spring Boot API 서버. 실시간 Kafka 스트리밍 기반 모니터링 파이프라인과 XGBoost 재점수화(ML
게이팅)까지 포함한다.

## 스택

Java 25 (Gradle toolchain) · Spring Boot 4.1 · Spring Data Neo4j · Spring Kafka · Gradle · Lombok

## 아키텍처

두 가지 축으로 구성된다.

**① 조사(batch) API** — 이미 적재된 전체 그래프를 조회하는 REST API. 컨트롤러 → 서비스 →
`Neo4jClient`로 직접 Cypher를 실행하는 구조이며, 가변 길이 경로 탐색·대용량 그래프의 조기 truncation
등 Neo4j 특유의 제약에 대한 대응이 포함되어 있다.

**② 실시간 모니터링 파이프라인** — `data/transactions.csv`를 시간순으로 리플레이하며 Kafka로 발행하고,
컨슈머가 슬라이딩 윈도우 기반 룰(순환 거래 / 집중 송금)로 실시간 탐지한다. 룰이 발화한 후보는 선택적으로
XGBoost 모델 서비스(`ml/`)에 재점수화를 요청해 오탐을 줄일 수 있고, 결과는 SSE로 프론트엔드에
브로드캐스트된다.

## 주요 API

| Method | Path | 설명 |
|---|---|---|
| GET | `/api/stats` | 대시보드 통계 |
| GET | `/api/accounts?keyword=` | 계좌 검색 |
| GET | `/api/accounts/{id}` | 계좌 상세 |
| GET | `/api/accounts/{id}/graph?depth=1..5` | 자금 흐름 그래프 |
| GET | `/api/patterns/circular\|layering\|fan-in\|fan-out` | 패턴 탐지 |
| GET | `/api/alerts` | Alert 목록/상세 |
| POST | `/api/monitor/start?rate=&days=` | 실시간 리플레이 시작 (`days`=재생할 시뮬레이션 일수, 0=전체) |
| POST | `/api/monitor/stop` \| `/reset` | 리플레이 중단 / 그래프·메트릭 초기화 |
| GET | `/api/monitor/stream` | SSE 스트림 (`transaction`/`alert`/`metrics` 이벤트) |
| GET | `/api/monitor/metrics` | 누적 TP/FP/FN, Precision/Recall |
| GET | `/api/monitor/missed` | 현재 놓친(FN) 사기 거래 목록 — SSE 버퍼가 아닌 서버 authoritative 소스 |
| GET/PUT | `/api/monitor/settings` | 탐지 임계값(윈도우, fan-in 최소 인원 등) 조회/실시간 변경 |

## 실행

```bash
docker compose up -d          # Neo4j + Kafka + ml + backend + frontend 전체 기동
```

개발 중 백엔드만 로컬로 띄우려면:

```bash
docker compose up -d neo4j kafka ml   # 의존 인프라만
./gradlew bootRun                     # :8080, devtools 핫리로드
```

- Neo4j 접속: `bolt://localhost:7687` (`neo4j` / `password123`)
- 최초 1회 데이터 적재: `docker cp src/main/resources/cypher/load.cypher money-flow-neo4j:/var/lib/neo4j/import/` 후 `cypher-shell`로 실행 (자세한 절차는 모노레포 루트 README 참고)

## 실시간 모니터링 상세

- **윈도우 탐지 규칙** (`monitor.*`, 런타임 변경 가능): 슬라이딩 윈도우 폭, fan-in 최소 송신자 수,
  순환 최대 홉 수, 의심 금액 상한, 금액 동일성 허용 오차. `PUT /api/monitor/settings`로 값을 바꾸면
  재시작 없이 **다음 이벤트부터 즉시** 새 기준이 적용된다(캐싱 없이 매 이벤트마다 값을 다시 읽는 구조).
- **ML 재점수화**: `monitor.model.enabled=true`로 켜면 룰 발화 후보마다 13개 피처를 추출해
  `ml/` 서비스에 점수를 요청하고, threshold 미만이면 알럿을 억제한다. ML 서비스 장애 시 fail-open
  (점수 1.0으로 간주해 알럿 유지)으로, 모델 문제가 탐지 누락으로 이어지지 않게 설계했다.
- **학습 데이터 수집**: `monitor.model.collectTrainingData=true`면 게이팅 이전의 모든 룰 후보를
  피처와 함께 CSV로 남겨, `ml/train.py`의 학습 입력으로 재사용한다.

## 테스트

```bash
./gradlew test
```

## 참고

전체 모노레포 개요·데이터셋 설명·엔드투엔드 실행 순서는 상위(모노레포) README를 참고.
