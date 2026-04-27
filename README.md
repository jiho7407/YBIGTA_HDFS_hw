# HDFS HA 과제

**HDFS High Availability 클러스터 구성 및 장애 복구 검증**

---

## 과제 개요

HDFS High Availability 클러스터를 직접 튜닝하고, 여러 장애 상황에서도 쓰기와 정합성이 유지되는지 확인하는 과제입니다.

수강생은 `student/` 디렉터리 안의 설정 파일만 수정합니다. 자동 검증 스크립트는 HDFS에 데이터를 쓰고 다시 읽어 hash를 비교하며, 공통 검증에서는 Active NameNode가 정확히 1개인지, `fsck` 기준 corrupt block이 없는지, 두 NameNode의 edit log txid가 일치하는지 확인합니다.

---

## 빠른 시작

### Docker 환경

```bash
./scripts/cluster.sh build
./scripts/cluster.sh init
```

상태 확인:

```bash
./scripts/cluster.sh status
```

전체 검증 실행:

```bash
./run_all.sh
```

결과 파일:

```
results/result.json
```

클러스터 정지 / 완전 초기화:

```bash
./scripts/cluster.sh down
./scripts/cluster.sh clean && ./scripts/cluster.sh init
```

### 준비 사항

- Docker Desktop 또는 Docker Engine + Docker Compose v2
- 최소 8GB RAM, Docker에 4GB 이상 할당
- 디스크 여유 공간 6GB 이상
- 빌드 시 인터넷 연결 필요 (GHCR에서 베이스 이미지를 pull합니다)

---

## 수강생이 해야 할 것

`student/` 안의 설정을 수정하여 HDFS HA 클러스터가 장애 상황을 견디도록 만드세요.

| 파일 | 설명 |
|---|---|
| `student/conf/hdfs-site.xml` | 주요 HA / failover / replication 튜닝 대상 |
| `student/conf/core-site.xml` | HDFS 기본 경로와 클라이언트 설정 |
| `student/conf/hadoop-env.sh` | Hadoop 데몬 환경 설정 |
| `student/conf/workers` | DataNode worker 목록 |
| `student/docker-compose.student.yml` | 서비스별 리소스와 환경 변수 조정 |

서비스 이름(`nn1`, `nn2`, `zk1~3`, `jn1~3`, `dn1~3`)은 바꾸지 마세요. 검증 시나리오가 이 이름을 기준으로 동작합니다.

---

## 검증 시나리오

| # | 시나리오 |
|---|---|
| 1 | Active NameNode clean kill |
| 2 | Active NameNode hard kill |
| 3 | Standby NameNode 장애 |
| 4 | JournalNode 1대 장애 |
| 5 | ZooKeeper 1대 장애 |
| 6 | Active NN ↔ ZooKeeper 단절 |
| 7 | DataNode 1대 장애 중 write |
| 8 | failover 중 대용량 write/read |
| 9 | 반복 chaos test |

---

## 디렉터리 구조

```
hdfs-ha-hw/
├── docker-compose.yml                 # 기본 HDFS HA 클러스터 구성
├── Dockerfile                         # Hadoop 이미지 (GHCR 베이스 이미지 사용)
├── Dockerfile.base                    # Hadoop 설치 레이어 (GHCR 배포용)
├── run_all.sh                         # 전체 검증 실행
├── scripts/                           # 클러스터 제어 및 데이터 주입 스크립트
├── scenarios/                         # 장애 검증 시나리오
├── validators/                        # 공통 HDFS 상태 검증
├── student/                           # ★ 이 안에서만 작업 ★
│   ├── conf/
│   │   ├── core-site.xml
│   │   ├── hdfs-site.xml
│   │   ├── hadoop-env.sh
│   │   └── workers
│   └── docker-compose.student.yml
├── results/                           # 실행 결과 JSON 생성 위치
└── submissions/                       # PR 제출 결과 파일 위치
```

---

## 채점 기준

```bash
./run_all.sh
```

| 항목 | 기준 |
|---|---|
| 시나리오 통과 | 9개 중 6개 이상 통과 |
| 데이터 정합성 | write/read hash 일치 |
| HA 상태 | Active NameNode 정확히 1개 |
| HDFS 상태 | corrupt block 없음 |
| edit log | 두 NameNode의 txid 일치 |

---

## 리더보드

통과한 제출은 ICPC scoreboard 스타일로 시나리오별 AC/WA와 실행 시간을 표시합니다.

순위는 아래 기준으로 매깁니다.

- 1순위: `scenarios_passed` 많을수록
- 2순위: `penalty_ms` 적을수록 (성공한 시나리오들의 실행 시간 합)

---

## 제출 방법

`submissions/<이름>/` 디렉터리를 만들고 `result.json`을 넣은 뒤 PR을 올리세요.

```
submissions/
└── 이름/
    └── result.json    # run_all.sh 실행 후 results/result.json 복사
```

PR을 올리면 GitHub Actions가 자동으로 검증합니다. CI를 통과해야 merge됩니다. merge되면 리더보드가 자동으로 업데이트됩니다.
