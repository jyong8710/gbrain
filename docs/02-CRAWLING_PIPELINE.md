# 부서 LLM 위키 — 크롤링 파이프라인 설계

> 목표: 사내 시스템/인프라/네트워크 문서를 자동으로 수집해 Markdown으로 변환하고, GBrain의 소스(source) 구조에 맞게 인입(ingest)한다. GBrain 자체는 크롤러를 제공하지 않으므로(README/34번 섹션 — 크롤링은 사용자가 직접 하고 md만 넘기면 그 다음은 GBrain이 처리), 이 파이프라인은 별도로 구축해야 하는 부분이다.

## 1. 전체 흐름

```
[사내 문서 소스들]           [크롤러/변환기]           [staging 디렉토리]        [GBrain 인입]
Confluence/Wiki  ───┐                                  infra-raw/
Git 레포 README  ───┤──▶ 소스별 어댑터 ──▶ MD 정규화 ──▶ voc-raw/      ──▶  gbrain sync
Grafana/모니터링 ───┤     (아래 3번)      + frontmatter    devops-raw/       (또는 jobs submit)
이슈트래커       ───┘                       주입                                 │
                                                                                 ▼
                                                                   pages/ + content_chunks/
                                                                   (임베딩 + 그래프 자동추출)
```

## 2. 소스 어댑터 설계 (Extract 단계)

| 원본 | 어댑터 방식 | 산출물 | 비고 |
|---|---|---|---|
| Confluence/사내 위키 | REST API로 페이지 목록+본문(HTML) 가져와 `turndown` 등으로 MD 변환 | `infra-raw/wiki/<space>/<page-slug>.md` | HTML 테이블/이미지 첨부는 별도 처리 필요 |
| Git 레포 (README, docs/, 코드 자체) | `git clone` 또는 이미 체크아웃된 레포를 그대로 소스로 등록 | 레포 자체가 소스 (변환 불필요) | **코드 자체도 `gbrain sources add`로 바로 등록 가능** — 8번/32번 섹션의 코드 인텔리전스 기능 활용 |
| Grafana/모니터링 대시보드 | 대시보드 정의(JSON) + 최근 알림 이력을 API로 가져와 요약 MD 생성 | `infra-raw/monitoring/<dashboard>.md` | 실시간 값 자체보다는 "이 대시보드가 뭘 보는지" 설명이 목적 |
| 이슈트래커 (Jira 등) | 완료된 incident/장애 티켓을 API로 가져와 MD 변환 | `devops-raw/incidents/<ticket-id>.md` | `incident` 타입 frontmatter로 매핑 |
| VOC 채널 (게시판/이메일) | 티켓/문의 원문을 MD로 | `voc-raw/tickets/<id>.md` | PII 마스킹 필요 여부 검토 (개인정보 포함 가능성) |

## 3. MD 정규화 규칙 (Transform 단계)

크롤링 직후 원본을 그대로 넣지 말고, 아래 정규화를 거쳐야 한다 (01-SCHEMA_DESIGN.md의 타입과 연결):

1. **Frontmatter 주입** — 각 문서에 타입/메타데이터 명시:
   ```yaml
   ---
   type: system
   title: 결제시스템
   owner_team: 결제팀
   criticality: critical
   source_url: https://confluence.internal/pages/12345
   crawled_at: 2026-09-06
   ---
   ```
2. **위키링크 변환** — 원본의 내부 하이퍼링크(Confluence 페이지 간 링크 등)를 GBrain이 인식하는 `[[systems/payment-system]]` 형식으로 변환. 이게 없으면 그래프 자동추출(23번 섹션)이 안 걸린다.
3. **관계 문구 보정** — 01번 문서의 관계 추출 정규식이 걸리도록, 가능하면 변환 스크립트가 원문의 구조화된 정보(예: Confluence의 "의존 시스템: X" 필드)를 `"본 시스템은 X에 의존한다"` 같은 정규식 매칭 가능한 문장으로 명시적으로 재작성. **원문을 왜곡하지 않는 선에서 보강**하는 것이지, 없는 관계를 지어내면 안 됨.
4. **PII/자격증명 스크러빙** — 크롤링 원본에 API 키, 패스워드, 개인정보가 섞여 있을 수 있음. `src/core/secret-scan.ts` 패턴(58.2/55번 섹션에서 확인된 `check-secret-scan` 관련 로직)을 참고해 인입 전 필수 스캔.

## 4. 인입 시 검증 절차 — Test-Before-Bulk 적용 (58.10 섹션)

크롤러가 처음 몇백~몇천 건을 한 번에 밀어넣는 상황이므로, GBrain 자체 컨벤션(`test-before-bulk.md`)을 그대로 적용:

```
Round 1 (10건): gbrain sync --dry-run → 카운트/필드 확인 → 3개 랜덤 샘플 육안 검토
Round 2 (100건): 실제 sync, 에러율 < 2% 확인, 처리량(items/sec) 측정
Round 3 (500건): 검색 가능 여부까지 확인 (gbrain search로 방금 넣은 페이지가 나오는지)
Round 4 (전체): --pace=gentle 옵션으로 페이싱, --progress-json으로 진행상황 모니터링
```

**흔한 실패 시그니처(58.10 섹션 사례)**: "에러 0건, 소요시간 정상"인데 실제로는 인코딩 깨짐/NOT NULL 위반으로 DB에 0행만 들어간 경우 — 반드시 **크롤링 전 카운트 vs 인입 후 실제 페이지 수**를 대조할 것.

## 5. 인입 명령 매핑

```bash
# infra 소스 등록 (최초 1회)
gbrain sources add infra --path ./staging/infra-raw --name "인프라/시스템 문서"
gbrain sources add voc --path ./staging/voc-raw --name "VOC 문의"
gbrain sources add devops --path ./staging/devops-raw --name "장애/운영 이력"

# 코드 레포는 별도 소스로 직접 등록 (변환 없이)
gbrain sources add payment-service-code --path /repos/payment-service --name "결제서비스 코드"

# 정기 동기화 (크롤러가 staging을 갱신한 뒤)
gbrain sync --source infra
gbrain sync --source voc
gbrain sync --source devops
```

## 6. 지속 갱신 (크롤러 재실행 주기)

- 크롤러 자체는 GBrain 밖의 **별도 스케줄러**(사내 CI/CD의 cron, 또는 GBrain의 Minions를 크롤러 실행기로 활용 — 58.2 섹션의 "cron은 반드시 Minion job으로" 컨벤션 참고 가능)로 주기 실행.
- Autopilot(13/28번 섹션)의 dream cycle이 밤마다 중복 페이지 정리/오래된 정보 탐지를 자동으로 하므로, 크롤러가 같은 문서를 매번 새로 만들어도 자동 정리가 어느 정도 보완해준다. 단, **slug(경로) 일관성**은 크롤러가 책임져야 한다 — 같은 Confluence 페이지가 크롤링마다 다른 slug로 저장되면 dedup이 안 걸림.

## 7. 다음 문서와의 연결

이 파이프라인의 출력(정규화된 MD + 소스 등록)이 03-ARCHITECTURE_PLAN.md의 "데이터 계층" 입력이 된다.
