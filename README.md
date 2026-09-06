# gbrain (부서 위키)

[GBrain](https://github.com/garrytan/gbrain)을 기반으로 구축하는 부서용 LLM 위키. 시스템/개발/운영 인프라/네트워크 지식을 저장하고, VOC 에이전트와 개발/운영 에이전트를 MCP로 Claude Code / 사내 LLM에 연결하는 것이 목표.

## 문서

- [`docs/department-wiki/01-SCHEMA_DESIGN.md`](docs/department-wiki/01-SCHEMA_DESIGN.md) — 페이지 타입/관계 타입 스키마 설계
- [`docs/department-wiki/02-CRAWLING_PIPELINE.md`](docs/department-wiki/02-CRAWLING_PIPELINE.md) — 사내 문서 크롤링/인입 파이프라인
- [`docs/department-wiki/03-ARCHITECTURE_PLAN.md`](docs/department-wiki/03-ARCHITECTURE_PLAN.md) — 전체 아키텍처 계획 (Postgres, MCP, 페르소나별 에이전트)
- [`docs/WIKI_IMPLEMENTATION_NOTES.md`](docs/WIKI_IMPLEMENTATION_NOTES.md) — GBrain 코드베이스 리버스엔지니어링 상세 문서 (아키텍처, 알고리즘, CLI 전체 카탈로그)

## 목표

- VOC 에이전트, 개발/운영 에이전트를 MCP로 Claude Code / 사내 LLM에 연결
- 시스템/인프라/네트워크 지식을 부서 공유 브레인으로 관리
- DB는 Postgres 확정 (팀 브레인 + Minions 동시성 요구사항 때문)

## 출처

이 저장소는 [garrytan/gbrain](https://github.com/garrytan/gbrain)을 기반으로 합니다. 원본 소스는 그대로 포함되어 있고(`src/`, `admin/`, `skills/` 등), 라이선스는 [`LICENSE`](LICENSE)(MIT)를 따릅니다. 원본 README는 [`docs/UPSTREAM_GBRAIN_README.md`](docs/UPSTREAM_GBRAIN_README.md)에 보관.
