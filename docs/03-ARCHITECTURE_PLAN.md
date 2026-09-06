# 부서 LLM 위키 — 전체 아키텍처 계획서

> 01-SCHEMA_DESIGN.md(무엇을 저장하나)와 02-CRAWLING_PIPELINE.md(어떻게 채우나)를 실제 배포 구조로 엮은 문서. GBrain 리버스엔지니어링 결과(`WIKI_IMPLEMENTATION_NOTES.md`)에서 검증된 사실만 근거로 삼는다.

## 1. 목표 재확인

- 데이터: 시스템/개발/운영 인프라/네트워크 정보 (부서 지식 저장소)
- 산출물: **VOC 에이전트**, **개발/운영 에이전트** — 둘 다 MCP로 Claude Code / 사내 LLM("가우스")에 연결
- 목표: 개발 생산성 + 장애대응 시너지
- 우선 기능: MCP, Autopilot, Minions, LongMemEval(품질검증), 팀 브레인, Memorable, 스키마 커스터마이징, 코드 인텔리전스, 한국어 검색설정
- DB: Postgres 확정

## 2. 전체 다이어그램

```
                          ┌─────────────────────────────────────┐
                          │   사내 서버 (Postgres + gbrain 상시서버) │
                          │                                       │
  [크롤러/CI]  ──sync──▶  │  Postgres (+ pgvector)               │
  (02번 문서)              │   - pages / content_chunks / links   │
                          │   - sources: infra / voc / devops     │
                          │   - minion_jobs (autopilot, 백그라운드)│
                          │                                       │
                          │  gbrain serve --http --port 3131      │
                          │   /mcp  (MCP 프로토콜 — 유일한 콘텐츠 API)│
                          │   /admin (운영자 대시보드, CRUD 아님)    │
                          └──────────────┬────────────────────────┘
                                         │ HTTPS + OAuth 2.1 (per-client scope)
                     ┌───────────────────┼───────────────────┐
                     ▼                   ▼                   ▼
            ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
            │  VOC 에이전트    │  │ 개발/운영 에이전트 │  │  관리자 클라이언트 │
            │  Claude Code /  │  │ Claude Code /   │  │  (본인, full)    │
            │  가우스(MCP지원시)│  │ 가우스           │  │                 │
            │  surface: starter│  │ surface: full   │  │                 │
            │  source: voc,   │  │ source: infra,  │  │                 │
            │  federated:infra│  │ devops (+voc?)  │  │                 │
            └────────────────┘  └────────────────┘  └────────────────┘
```

## 3. 배포 결정 사항 (README/2/11/14/45번 섹션 근거)

### 3.1 DB — Postgres (Supabase 또는 자체호스팅)
- PGLite(기본, 임베디드)는 **단일 프로세스 전제**라 팀 브레인·Minions와 근본적으로 안 맞는다(27번 섹션 실측: "동시 접근성"이 유일한 진짜 차이). Postgres로 처음부터 간다는 결정은 옳다.
- 벡터(pgvector)와 그래프(재귀 CTE)가 전부 Postgres 하나에 통합되어 있어 별도 벡터DB/그래프DB 불필요.
- `GBRAIN_FTS_LANGUAGE`를 한국어 관련 설정으로 맞추고(31번/58.8번 섹션 검토), 필요 시 `unaccent`+커스텀 tsconfig 조합 검토.

### 3.2 서버 상시 기동 — `gbrain serve --http`
- 로컬(stdio) 모드(`gbrain serve`)는 **팀 공유 불가**(개인 로컬 프로세스). 반드시 HTTP 모드로 사내 서버/컨테이너에 상시 배포.
- `/mcp`가 콘텐츠 조작의 **유일한 프로그래밍 경로**다(11번 섹션 — REST API 없음). 자체 CRUD 포탈을 만들고 싶다면 MCP 클라이언트 SDK로 `/mcp`를 호출하거나 얇은 REST↔MCP 프록시를 직접 만들어야 한다.
- `/admin`은 콘텐츠 CRUD가 아니라 OAuth 클라이언트/요청로그/job 모니터링 전용(11/38/43/47번 섹션) — 운영자용 대시보드로만 활용.

### 3.3 소스(Source) 분리 — 01번 문서와 동일
`infra` / `voc` / `devops` 3개 소스. 소스 분리가 곧 **접근 스코핑의 단위**(25/45.4번 섹션 — OAuth 클라이언트가 SQL WHERE절 `source_id = ANY($N)`로 강제됨, 애플리케이션 레벨이 아니라 DB 레벨 격리).

### 3.4 페르소나별 MCP 클라이언트 — 45.4번 섹션 근거

| 클라이언트 | surface | source (쓰기) | federated-read | 근거 |
|---|---|---|---|---|
| VOC 에이전트 | `starter` (27개 오퍼레이션, `add_link`/`remove_link` 등 그래프 쓰기 제외) | `voc` | `voc, infra` | VOC가 실수로 지식그래프를 조작 못 하게 하는 기본 설계와 일치 |
| 개발/운영 에이전트 | `full` | `devops` | `infra, devops` (+ 필요 시 `voc`) | 장애대응 시 코드 인텔리전스/전체 오퍼레이션 필요 |
| 관리자(본인) | `full` | 전체 | 전체 | `gbrain remote doctor` 등 운영 커맨드용 |

등록 절차(45.4/14번 섹션):
```bash
gbrain serve --http --port 3131 --bind 0.0.0.0 --public-url https://brain.internal.company.com

gbrain agent register voc-bot --harness claude-code --preset daily-driver \
  --url https://brain.internal.company.com/mcp
# 등록 후 --source voc --federated-read voc,infra --surface starter 로 rescope

gbrain agent register devops-bot --harness claude-code \
  --url https://brain.internal.company.com/mcp
# --source devops --federated-read infra,devops --surface full 로 rescope
```

**"가우스" 연동 시 반드시 먼저 확인**: 사내 LLM 플랫폼이 MCP 클라이언트(도구 호출 프로토콜)를 지원하는지. 지원 안 하면 `/mcp`를 감싸는 자체 브리지 서버가 필요(REST API가 원래 없다는 3.2번 항목과 연결되는 동일한 제약).

### 3.5 우선 기능 반영

| 기능 | 배포 방식 |
|---|---|
| Autopilot | `gbrain autopilot --install`로 cron 등록, 매일 밤 dream cycle(22개 페이즈, 28번 섹션) 실행 |
| Minions | `gbrain jobs work --job-isolation process`로 워커 상시 실행 — 장애대응 중 "이 로그 전체 분석" 같은 장시간 작업을 백그라운드 큐로 처리 |
| LongMemEval | 상시 기능 아님. 초기 데이터 적재 후 `gbrain eval longmemeval` 및 `gbrain eval brainbench`로 검색 품질 검증하는 **QA 단계**로 CI 또는 정기 점검에 편입 |
| 팀 브레인 | 3.3/3.4번 항목으로 이미 반영 |
| Memorable | 선택적, 3단계 동의 절차(`memorable init` → `enable` → `gbrain config set integrations.memorable.enabled true`) — 개발/운영 에이전트가 "지난번 이 장애 어떻게 고쳤는지" 절차 기억하는 용도로 나중 단계에 적용 검토 |
| 스키마 커스터마이징 | 01번 문서 |
| 코드 인텔리전스 | devops 소스에 실제 코드 레포 등록 — 단, **TS/TSX/JS/Python 4개 언어만 호출그래프(code_callers 등) 지원**(42번 섹션). 다른 언어(Go/Java 등)는 검색만 되고 그래프는 안 됨 — 스택에 맞게 기대치 조정 필요 |
| 한국어 검색설정 | `GBRAIN_FTS_LANGUAGE` 설정 + 31/58.8번 섹션의 salience/recency 영어전용 이슈를 스킬 프롬프트에서 명시적 파라미터로 우회 |

## 4. 인프라 요구사항 (실측 기반)

- Postgres 16+ (pgvector 확장)
- gbrain 바이너리(Bun 컴파일) 또는 소스 실행
- 상시 서버 1대(컨테이너 가능) — `gbrain serve --http` + Minions 워커 프로세스
- HTTPS 종단(리버스 프록시 또는 로드밸런서) — OAuth 2.1이 HTTPS 전제
- 크롤러 실행 환경(02번 문서) — 별도 배치/스케줄러 서버 또는 Minion job으로 통합

## 5. 리스크 요약 (전체 문서군 종합)

1. **REST API 부재** — 자체 프론트엔드 포탈은 MCP 경유 필수, 프론트 개발자 학습곡선 있음
2. **한국어 자동 신호(salience/recency)** — 명시적 파라미터 지정 필요, 안 하면 "최근 장애" 질의가 최신순으로 안 잡힐 수 있음
3. **코드 인텔리전스 언어 제한** — TS/JS/Python 외 언어는 그래프 기능 없음
4. **admin 콘솔이 CRUD가 아님** — 별도 프론트엔드 필요 시 개발 공수 별도 산정 필요
5. **관계 자동추출이 정규식 기반** — 한국어 문장 패턴 커버리지를 초기엔 넓게 잡고 반복 보강 필요(01번 문서)

## 6. 다음 단계 체크리스트

- [ ] Postgres 인스턴스 프로비저닝 (+ pgvector)
- [ ] `gbrain init` (Postgres 모드로)
- [ ] 01번 문서의 스키마팩 적용
- [ ] 02번 문서의 크롤러 최소 1개 소스(예: Confluence) 파일럿 실행 → 10건 테스트
- [ ] `gbrain serve --http` 상시 배포 + HTTPS
- [ ] VOC/개발운영 클라이언트 등록 및 스코프 검증(Alice/Bob 격리 테스트, 14번 섹션 방식)
- [ ] "가우스" MCP 지원 여부 확인 → 브리지 필요시 설계 착수
- [ ] Autopilot 설치
- [ ] 초기 데이터 적재 후 `gbrain eval brainbench` / `longmemeval`로 품질 확인
