# 부서 LLM 위키 — 스키마 설계

> GBrain의 "Agent-authored schema" 기능(WIKI_IMPLEMENTATION_NOTES.md 9번/33번/58.11 섹션)을 사용해, 기본 스키마(person/company/meeting 등 22개 범용 타입) 대신 부서 인프라/시스템 도메인에 맞는 전용 타입을 정의한다. 스키마 없이 크롤링한 문서를 그냥 넣으면 전부 `note` 타입으로 들어가 그래프/전문화 검색을 못 쓰게 되므로, **설치 직후 가장 먼저 해야 할 작업**이다.

## 1. 페이지 타입 (Page Types)

| 타입 | 용도 | primitive | prefix | extractable |
|---|---|---|---|---|
| `system` | 사내 시스템/서비스 단위 (예: "결제시스템", "회원DB") | entity | `systems/` | O |
| `service` | system보다 세분화된 개별 마이크로서비스/컴포넌트 | entity | `services/` | O |
| `network-device` | 라우터/스위치/방화벽 등 네트워크 장비 | entity | `network/` | O |
| `server` | 물리/가상 서버, VM, 컨테이너 호스트 | entity | `servers/` | O |
| `incident` | 장애 이력 | temporal | `incidents/` | O |
| `runbook` | 장애 대응/운영 절차서 | entity | `runbooks/` | X (사람이 직접 씀, 자동추출 대상 아님) |
| `deployment` | 배포/릴리스 기록 | temporal | `deployments/` | O |
| `voc-ticket` | 고객 문의/이슈 | temporal | `voc/` | O |
| `credential-policy` | 인증/자격증명 정책 문서 (비밀값 자체는 저장 금지 — 정책만) | entity | `security/` | X |

**설계 원칙 (58.9/schema-evolution.md 기준)**: 클러스터가 20개 미만이면 타입을 새로 만들지 말고 기존 타입 + frontmatter 태그로 시작. 20~100개면 alias나 narrow prefix. 100개 이상이면 위 표처럼 1급 타입으로 승격. 처음엔 `system`/`incident`/`runbook` 3개만 만들고, 데이터가 쌓이면서 `service`/`network-device`로 세분화하는 점진적 접근을 권장.

## 2. 관계 타입 (Typed Edges)

GBrain의 자동 그래프 추출(23번 섹션)은 **LLM 호출 없이 100% 규칙 기반**이다 — 정규식 + 가제티어(엔티티명 사전) 매칭이며, 타입 있는 관계는 **스키마팩에 정규식 패턴으로 선언**해야 자동 추출된다.

| 관계 | 의미 | 자동추출 트리거 패턴 예시 |
|---|---|---|
| `depends_on` | A 시스템이 B에 의존 | `"X는 Y에 의존한다"`, `"X depends on Y"`, `"X requires Y"` |
| `hosted_on` | A가 B 서버/인프라에서 구동 | `"X는 Y에서 구동된다"`, `"X runs on Y"`, `"X hosted on Y"` |
| `owned_by` | A 시스템의 담당팀/담당자 | `"X 담당자는 Y"`, `"owned by Y"` |
| `caused_by` | incident가 특정 원인/변경에 의해 발생 | `"root cause: Y"`, `"Y로 인해 발생"` |
| `resolved_by` | incident가 특정 runbook/조치로 해결 | `"Y 절차로 해결"`, `"resolved via Y"` |
| `connects_to` | 네트워크 장비 간 연결 관계 | `"X는 Y와 연결"`, `"X connects to Y"` |
| `impacts` | incident가 특정 시스템/서비스에 영향 | `"Y에 영향"`, `"impacts Y"` |

**주의(23.C 섹션 재확인)**: 이 패턴은 정규식이라 **한국어 어순 변화**(조사 위치, 어순 도치)에 취약할 수 있다. 초기엔 patterns를 넓게 잡고(false negative 감수), `gbrain schema lint` / `gbrain doctor`로 놓친 관계가 많은지 주기적으로 확인 후 패턴을 보강하는 반복 개선이 필요하다.

## 3. 스키마팩 초기 골격 (실제 YAML 형태, 33번 섹션 기준)

```yaml
# schema-packs/dept-infra-v1.yaml
name: dept-infra-v1
page_types:
  - name: system
    primitive: entity
    prefix: systems/
    extractable: true
    fields:
      - name: owner_team
        type: string
      - name: criticality
        type: enum
        values: [critical, high, medium, low]
  - name: incident
    primitive: temporal
    prefix: incidents/
    extractable: true
    fields:
      - name: severity
        type: enum
        values: [sev1, sev2, sev3, sev4]
      - name: affected_system
        type: string
      - name: root_cause
        type: string
link_types:
  - name: depends_on
    inference:
      regex: ["(.+)\\s*(?:는|은|이|가)?\\s*(.+)\\s*에\\s*의존", "(.+)\\s+depends?\\s+on\\s+(.+)"]
  - name: hosted_on
    inference:
      regex: ["(.+)\\s*(?:는|은)?\\s*(.+)\\s*에서\\s*(?:구동|실행)", "(.+)\\s+(?:runs?|hosted)\\s+on\\s+(.+)"]
```

적용 순서:
```bash
gbrain schema add-type system --primitive entity --prefix systems/ --extractable
gbrain schema add-type incident --primitive temporal --prefix incidents/ --extractable
gbrain schema add-type runbook --primitive entity --prefix runbooks/
gbrain schema add-link-type depends_on
gbrain schema add-link-type hosted_on
gbrain schema lint          # 정합성 검사
gbrain schema sync --apply  # 기존 페이지 backfill (1000행 배치)
```

## 4. 소스(Source) 분리와의 관계

스키마는 **브레인 전체에 적용**되지만(소스 무관), 실제 데이터 접근 스코핑은 소스 단위다. 3개 소스로 분리 권장:

| 소스 | 담당 타입 | 접근 대상 |
|---|---|---|
| `infra` | system, service, network-device, server, deployment, credential-policy | 개발/운영 에이전트 (full), VOC 에이전트 (읽기만, 제한적) |
| `voc` | voc-ticket | VOC 에이전트 (읽기/쓰기), 개발/운영 에이전트 (읽기만, 필요시) |
| `devops` | incident, runbook, deployment(중복 가능) | 개발/운영 에이전트 (full) |

02-CRAWLING_PIPELINE.md와 03-ARCHITECTURE_PLAN.md에서 이 소스 분리를 실제 배포 구조에 연결한다.

## 5. 한국어 관련 스키마 설계 리스크 (15/31/58.8 섹션 종합)

- **긍정적**: CJK 토큰 처리(한글 완성형 음절 U+AC00-D7AF)와 조사/어순을 다루는 `splitCJKQueryTerms()` 같은 전용 함수가 이미 존재 — 검색 자체는 우려보다 나음.
- **리스크로 남는 것**:
  1. `salience`/`recency` 자동감지 휴리스틱은 **영어 전용** — "최근 장애", "이번 주 배포" 같은 한국어 질의는 자동으로 최신성 가중이 안 걸림. VOC/운영 에이전트의 MCP 호출 시 `recency`/`salience` 파라미터를 **명시적으로** 넘기도록 스킬/프롬프트에서 강제해야 함.
  2. 관계 추출 정규식이 한국어 어순 대응 안 됨 — 초기 패턴 커버리지가 낮을 수 있음(위 3번 참고).
  3. 문장 분리(마침표 등) 자체는 한국어 전용 처리가 없음 — 청킹 경계가 어색할 수 있음(21번/31번 섹션).
