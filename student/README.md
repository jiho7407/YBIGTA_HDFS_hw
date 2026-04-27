# 수정 가능한 파일

| 파일 | 역할 |
|---|---|
| `conf/hdfs-site.xml` | HA failover, JournalNode 연동, 타임아웃 등 핵심 HDFS 설정 |
| `conf/hadoop-env.sh` | NameNode/DataNode JVM 옵션, heap 크기 |
| `docker-compose.student.yml` | 컨테이너 리소스 제한, 환경 변수 |

수정하면 안 되는 것:

- `conf/workers`
- `docker-compose.student.yml`의 서비스 이름 (`nn1`, `nn2`, `zk1~3`, `jn1~3`, `dn1~3`)
- `../scenarios/`, `../validators/`, `../scripts/`, `../run_all.sh`, `../docker-compose.yml`

---

## hdfs-site.xml 튜닝 항목

`hdfs-site.xml`의 `TODO` 블록에 아래 속성들을 추가하거나 조정해야 합니다.

자동 Failover 설정인 `dfs.ha.automatic-failover.enabled=true`는 클러스터 초기화가 가능하도록
기본 템플릿에 이미 들어 있습니다. 해당 값은 삭제하지 마세요.

### Standby 동기화

| 속성 | 설명 |
|---|---|
| `dfs.ha.tail-edits.in-progress` | in-progress segment도 읽어서 Standby가 빠르게 따라잡음 |
| `dfs.ha.tail-edits.period` | 동기화 주기 (기본값은 60s) |

### Failover/재시도 타이밍 (선택)

| 속성 | 기본값 |
|---|---|
| `ha.zookeeper.session-timeout.ms` | 5000 |
| `dfs.client.failover.sleep.base.millis` | 500 |
| `dfs.client.failover.sleep.max.millis` | 15000 |
| `dfs.client.failover.max.attempts` | 15 |
| `ipc.client.connect.max.retries` | 10 |

참고: [HDFS HA with QJM 공식 문서](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html)
