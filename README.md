# gbrain (부서 위키 구현)

GBrain(https://github.com/garrytan/gbrain) 기반 부서용 LLM 위키 구현 프로젝트.

## 문서

- [`docs/01-SCHEMA_DESIGN.md`](docs/01-SCHEMA_DESIGN.md) — 페이지 타입/관계 타입 스키마 설계
- [`docs/02-CRAWLING_PIPELINE.md`](docs/02-CRAWLING_PIPELINE.md) — 사내 문서 크롤링/인입 파이프라인
- [`docs/03-ARCHITECTURE_PLAN.md`](docs/03-ARCHITECTURE_PLAN.md) — 전체 아키텍처 계획 (Postgres, MCP, 페르소나별 에이전트)

## 목표

- VOC 에이전트, 개발/운영 에이전트를 MCP로 Claude Code / 사내 LLM에 연결
- 시스템/인프라/네트워크 지식을 부서 공유 브레인으로 관리
