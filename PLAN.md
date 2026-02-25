# PerpDEX Arbitrage Alarm Bot - 종합 계획서

## 1. 프로젝트 개요

7개 PerpDEX(Hyperliquid, Variational, Lighter, Extended, Paradex, 01.xyz, Nado) 간의 실시간 가격 차이를 모니터링하고, 설정된 임계값을 초과하면 알림을 보내는 봇.

### 지원 페어 (Phase 1)
- BTC-PERP, ETH-PERP, HYPE-PERP, SOL-PERP, BNB-PERP

### 핵심 요구사항
- **100ms (0.1초) 이내 가격 트래킹**
- 거래소 추가 용이한 플러그인 아키텍처
- 페어 추가 용이한 설정 기반 구조

---

## 2. 거래소별 API 분석

| 거래소 | API 타입 | WebSocket 엔드포인트 | 가격 업데이트 주기 | Rate Limit | BTC | ETH | SOL | HYPE | BNB |
|---------|----------|---------------------|-------------------|------------|-----|-----|-----|------|-----|
| **Hyperliquid** | WS + REST | `wss://api.hyperliquid.xyz/ws` | 실시간 (AllMids, event-driven) | 1000 WS subs/IP, 1200 wt/min REST | O | O | O | O | O |
| **Paradex** | WS + REST | `wss://ws.api.prod.paradex.trade/v1/` | **50ms** (BBO 채널) | 200 req/s REST, 20 WS conn/s | O | O | O | O | O |
| **Nado** | WS + REST | `wss://gateway.prod.nado.xyz/v1/subscribe` | **~50ms** book, event-driven BBO | 120 query/min, 600 order/min | O | O | O | ? | O |
| **Lighter** | WS + REST | `wss://mainnet.zklighter.elliot.ai/stream` | **50ms** 배치 | 24,000 wt/60s (premium) | O | O | O | O | ? |
| **Extended** | WS + REST | `wss://api.starknet.extended.exchange/...` | **100ms** push | 1,000 req/min REST | O | O | O | ? | ? |
| **01.xyz** | REST (로컬) | 미문서화 (베타) | 미확인 | 미문서화 (베타) | O | ? | O | ? | ? |
| **Variational** | REST only | 없음 (개발중) | 최대 600s 캐시 | 10 req/10s | O | O | O | O | O |

### 거래소 Tier 분류

**Tier 1 — 즉시 운영 가능, 최적:**
1. **Hyperliquid**: 가장 성숙한 API. `allMids` WS로 전체 자산 mid price 실시간 수신. 5개 페어 모두 지원.
2. **Paradex**: BBO 50ms refresh, JSON-RPC 2.0, REST 200req/s. 5개 페어 모두 지원. 소매 수수료 무료.
3. **Nado**: 5-15ms 매칭 엔진, 50ms book depth, Kraken 팀 제작. TS/Py/Rust SDK. HYPE 미확인.

**Tier 2 — 양호, 일부 제약:**
4. **Lighter**: 50ms OB 배치, 수수료 무료. BNB 미확인.
5. **Extended**: 100ms OB push. Starknet 기반. BNB/HYPE 미확인.

**Tier 3 — 현재 부적합:**
6. **01.xyz**: 베타 단계. 로컬 자체 호스팅 REST. 공개 WS 문서 없음.
7. **Variational**: REST only, 600초 캐싱, 10req/10s. 실시간 트래킹 근본적으로 불가.

### 주요 리스크
- **Variational**: WebSocket 미지원, REST rate limit 매우 낮음 (1req/s). 100ms 트래킹 불가능 → 1초 간격 폴링으로 대체
- **01.xyz**: 베타 단계, 자체 호스팅 REST API 모델. 프로덕션 운영 시 추가 조사 필요
- **일부 거래소**: 특정 페어 미지원 가능성 (HYPE — Nado/Extended/01.xyz, BNB — Lighter/Extended/01.xyz)

---

## 3. 기술 스택

### 언어: TypeScript (Node.js)
**선택 이유:**
- 네이티브 WebSocket 지원 (Node.js 22+)
- 비동기 I/O에 최적화 (이벤트 루프)
- 모든 대상 거래소가 TS/JS SDK 제공
- JSON 네이티브 처리
- npm 생태계 (ws, ccxt 등)

### 대안 고려:
| 언어 | 장점 | 단점 |
|------|------|------|
| Python (asyncio) | 빠른 프로토타이핑, SDK 풍부 | GIL로 인한 CPU 병목 가능 |
| Rust | 최고 성능, 메모리 안전 | 개발 속도 느림, SDK 제한적 |
| Go | 동시성 우수, 빠른 빌드 | SDK 지원 부족 |

### 핵심 라이브러리
```
ws                    # WebSocket 클라이언트 (가장 빠름)
ccxt                  # 거래소 통합 (Hyperliquid 등 일부 지원)
@hyperliquid/sdk      # Hyperliquid 공식 SDK
@n1xyz/nord-ts        # 01.xyz 공식 SDK
pino                  # 고성능 로거
dotenv                # 환경 변수 관리
telegraf / slack-bolt # 알림 (Telegram / Slack)
```

---

## 4. 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                     Price Monitor Bot                        │
│                                                              │
│  ┌──────────────────────────────────────────────────┐       │
│  │           Exchange Adapter Layer                   │       │
│  │                                                    │       │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐            │       │
│  │  │Hyperliq │ │ Lighter │ │ Paradex │  ...        │       │
│  │  │Adapter  │ │ Adapter │ │ Adapter │             │       │
│  │  └────┬────┘ └────┬────┘ └────┬────┘            │       │
│  │       │           │           │                   │       │
│  │  ┌────▼───────────▼───────────▼────┐             │       │
│  │  │    Unified Price Interface       │             │       │
│  │  │    { exchange, pair, bid,        │             │       │
│  │  │      ask, mid, timestamp }       │             │       │
│  │  └────────────┬────────────────┘             │       │
│  └───────────────┼──────────────────────────────┘       │
│                  │                                        │
│  ┌───────────────▼──────────────────────────────┐       │
│  │          Price Aggregator                      │       │
│  │                                                │       │
│  │  ┌─────────────┐  ┌──────────────────┐        │       │
│  │  │ Price Store  │  │ Spread Calculator │        │       │
│  │  │ (In-Memory)  │  │ (Pairwise Diff)  │        │       │
│  │  └──────┬──────┘  └────────┬─────────┘        │       │
│  │         │                  │                    │       │
│  │  ┌──────▼──────────────────▼─────────┐        │       │
│  │  │       Threshold Engine             │        │       │
│  │  │  - Absolute diff (e.g. > $50)      │        │       │
│  │  │  - Percentage diff (e.g. > 0.3%)   │        │       │
│  │  │  - Sustained period (e.g. > 5s)    │        │       │
│  │  └──────────────┬────────────────────┘        │       │
│  └─────────────────┼────────────────────────────┘       │
│                    │                                      │
│  ┌─────────────────▼────────────────────────────┐       │
│  │          Alert System                          │       │
│  │                                                │       │
│  │  ┌──────────┐ ┌───────┐ ┌─────────┐          │       │
│  │  │ Telegram │ │ Slack │ │ Discord │ ...       │       │
│  │  └──────────┘ └───────┘ └─────────┘          │       │
│  └──────────────────────────────────────────────┘       │
│                                                          │
│  ┌──────────────────────────────────────────────┐       │
│  │          Monitoring & Logging                  │       │
│  │  - Health checks per exchange connection       │       │
│  │  - Latency tracking per feed                   │       │
│  │  - Alert rate / cooldown management            │       │
│  │  - Dashboard (optional: Grafana)               │       │
│  └──────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

### 핵심 설계 원칙

1. **Adapter Pattern**: 각 거래소별 어댑터가 통일된 인터페이스(`ExchangeAdapter`)를 구현
2. **Event-Driven**: 가격 업데이트가 EventEmitter를 통해 전파
3. **In-Memory Price Store**: Map 기반으로 최신 가격 저장 (DB 불필요, 지연 최소화)
4. **Pairwise Comparison**: N개 거래소에서 nC2 조합으로 스프레드 계산

---

## 5. 디렉토리 구조

```
perpdex_arbitrage_alarm/
├── src/
│   ├── index.ts                    # 엔트리포인트
│   ├── config/
│   │   ├── exchanges.ts            # 거래소 설정 (활성화/비활성화, 엔드포인트)
│   │   ├── pairs.ts                # 모니터링 페어 목록
│   │   └── thresholds.ts           # 임계값 설정
│   ├── adapters/                   # 거래소별 어댑터
│   │   ├── base.ts                 # BaseExchangeAdapter (추상 클래스)
│   │   ├── hyperliquid.ts
│   │   ├── lighter.ts
│   │   ├── paradex.ts
│   │   ├── 01xyz.ts
│   │   ├── nado.ts
│   │   ├── extended.ts
│   │   └── variational.ts         # REST 폴링 방식
│   ├── core/
│   │   ├── price-store.ts          # 인메모리 가격 저장소
│   │   ├── spread-calculator.ts    # 스프레드 계산 엔진
│   │   ├── threshold-engine.ts     # 임계값 판단 로직
│   │   └── connection-manager.ts   # WS 연결 관리 (재연결 등)
│   ├── alerts/
│   │   ├── base.ts                 # BaseAlertProvider
│   │   ├── telegram.ts
│   │   ├── slack.ts
│   │   └── discord.ts
│   ├── monitoring/
│   │   ├── health-check.ts         # 각 거래소 연결 상태 체크
│   │   ├── latency-tracker.ts      # 피드별 레이턴시 추적
│   │   └── metrics.ts              # Prometheus 메트릭 (선택)
│   └── types/
│       ├── price.ts                # PriceUpdate, Spread 등 타입
│       └── config.ts               # 설정 타입
├── tests/
├── .env.example
├── package.json
├── tsconfig.json
└── Dockerfile
```

---

## 6. 핵심 타입 정의

```typescript
interface PriceUpdate {
  exchange: string;       // "hyperliquid" | "lighter" | ...
  pair: string;           // "BTC-PERP" | "ETH-PERP" | ...
  bid: number;
  ask: number;
  mid: number;
  timestamp: number;      // ms since epoch
  receivedAt: number;     // 로컬 수신 시각 (레이턴시 측정용)
}

interface SpreadAlert {
  pair: string;
  exchangeA: string;
  exchangeB: string;
  priceA: number;
  priceB: number;
  spreadAbsolute: number; // 절대 차이 ($)
  spreadPercent: number;  // 퍼센트 차이 (%)
  direction: 'A>B' | 'B>A';
  timestamp: number;
}

interface ThresholdConfig {
  pair: string;
  minSpreadPercent: number;     // e.g. 0.3 (= 0.3%)
  minSpreadAbsolute?: number;   // e.g. 50 ($)
  sustainedMs?: number;         // 지속 시간 (ms), 노이즈 필터링
  cooldownMs: number;           // 동일 알림 재전송 대기 시간
}
```

---

## 7. 데이터 흐름 (100ms 이내 달성 전략)

### Step 1: WebSocket 연결 (시작 시 1회)
```
Bot 시작 → 7개 거래소 동시 WebSocket 연결
         → 각 페어별 BBO/Price 채널 구독
         → Variational만 REST 폴링 (1초 간격)
```

### Step 2: 가격 수신 (실시간)
```
Exchange WS → Adapter (파싱 + 정규화) → PriceStore.update()
                                        (< 1ms, 인메모리 Map)
```

### Step 3: 스프레드 계산 (가격 수신 시마다)
```
PriceStore.update() → SpreadCalculator.calculate()
                      (해당 페어의 모든 거래소 쌍 비교)
                    → ThresholdEngine.check()
                    → [조건 충족 시] AlertSystem.send()
```

### 레이턴시 예상치
| 구간 | 예상 시간 |
|------|-----------|
| WS 메시지 수신 | 10-50ms (네트워크) |
| JSON 파싱 + 정규화 | < 0.5ms |
| 가격 저장 (Map) | < 0.01ms |
| 스프레드 계산 (7C2 = 21쌍 × 5페어) | < 0.1ms |
| 임계값 체크 | < 0.01ms |
| **총 처리 시간 (네트워크 제외)** | **< 1ms** |

> 100ms 트래킹이 가능하려면, 핵심은 **네트워크 레이턴시**를 줄이는 것.
> 계산 자체는 1ms 미만으로 충분히 빠름.

---

## 8. 인프라 / 서버 권장 사항

### Option A: AWS (권장)

| 리소스 | 스펙 | 월 비용 (예상) | 용도 |
|--------|------|---------------|------|
| **EC2 인스턴스** | `t3.medium` (2 vCPU, 4GB) | ~$30/월 | 봇 실행 |
| **리전** | `ap-northeast-1` (도쿄) 또는 `us-east-1` (버지니아) | - | 거래소 서버 근접 |
| **EBS** | 20GB gp3 | ~$2/월 | OS + 로그 |
| **CloudWatch** | 기본 | 무료~$5/월 | 모니터링, 알림 |
| **총 예상** | | **~$35-40/월** | |

#### 리전 선택 가이드
- **Hyperliquid**: 서버 위치 공개 안됨 (추정 US)
- **Nado**: Ink L2 기반 (추정 US/EU)
- **대부분의 거래소**: US 또는 EU 서버
- **권장**: `us-east-1` (버지니아) — 대부분의 거래소와 가장 가까울 확률 높음

#### 네트워크 최적화
- **Enhanced Networking** 활성화 (ENA)
- **Placement Group** 사용 시 네트워크 레이턴시 추가 개선
- 필요 시 `c6i.large` (컴퓨팅 최적화)로 업그레이드: ~$60/월

### Option B: VPS (저비용)
| 서비스 | 스펙 | 월 비용 |
|--------|------|--------|
| Hetzner | 4GB RAM, 2 vCPU | ~$5-7/월 |
| DigitalOcean | 4GB RAM, 2 vCPU | ~$24/월 |
| Vultr | 4GB RAM, 2 vCPU | ~$24/월 |

> 저비용이지만 네트워크 품질이 AWS보다 떨어질 수 있음

### Option C: 로컬 실행 (개발/테스트)
- 개발 단계에서는 로컬에서 충분
- 실제 운영은 서버 권장 (24/7 안정성)

---

## 9. 알림 시스템

### Telegram Bot (권장 - 1순위)
- 무료
- 모바일 푸시 알림
- 그룹/채널 지원
- 메시지 포맷팅 (Markdown)

### 알림 메시지 예시
```
🚨 SPREAD ALERT: BTC-PERP

Hyperliquid: $67,234.50 (bid)
Lighter:     $67,312.80 (ask)

Spread: $78.30 (0.116%)
Direction: Buy HL → Sell Lighter

Duration: 3.2s
Time: 2026-02-25 14:32:05 UTC
```

### 알림 제어
- **Cooldown**: 동일 페어/거래소 조합은 최소 30초 간격
- **Escalation**: 스프레드가 커질수록 알림 빈도 증가
- **Quiet Hours**: 설정 가능한 무음 시간대
- **Summary**: 매시간 스프레드 요약 리포트

---

## 10. 추가 권장 기능

### 10-1. 펀딩레이트 차익 모니터링
거래소 간 펀딩레이트 차이도 차익거래 기회. 각 거래소의 funding rate를 함께 트래킹.

### 10-2. 히스토리컬 데이터 저장
- SQLite 또는 TimescaleDB로 가격/스프레드 히스토리 저장
- 패턴 분석, 백테스팅에 활용
- 선택적 기능 (Phase 2)

### 10-3. 웹 대시보드
- 실시간 가격 테이블
- 스프레드 차트 (시계열)
- 거래소별 연결 상태
- 기술: React + WebSocket → 브라우저

### 10-4. 자동 매매 연동 (Phase 3)
- 알림 대신 직접 주문 실행
- 양쪽 거래소에 동시 주문 (buy low, sell high)
- 리스크 관리 모듈 필수

### 10-5. 슬리피지 / 유동성 체크
- BBO 가격 뿐 아니라 실제 실행 가능 수량까지 확인
- L2 오더북 데이터로 슬리피지 예상치 계산

### 10-6. 멀티 체인 가스비 모니터링
- 실제 차익거래 시 가스비가 수익을 초과할 수 있음
- 체인별 가스비를 함께 모니터링하여 순수익 계산

---

## 11. 개발 로드맵

### Phase 1: 핵심 기능 (1-2주)
- [ ] 프로젝트 세팅 (TypeScript, 빌드, 린트)
- [ ] 타입 정의 및 인터페이스 설계
- [ ] BaseExchangeAdapter 추상 클래스
- [ ] Hyperliquid 어댑터 (가장 안정적인 API)
- [ ] Lighter 어댑터
- [ ] PriceStore + SpreadCalculator
- [ ] ThresholdEngine
- [ ] Telegram 알림
- [ ] 기본 로깅

### Phase 2: 거래소 확장 (1-2주)
- [ ] Paradex 어댑터
- [ ] 01.xyz 어댑터
- [ ] Nado 어댑터
- [ ] Extended 어댑터
- [ ] Variational 어댑터 (REST 폴링)
- [ ] 연결 상태 모니터링 / 자동 재연결
- [ ] 레이턴시 트래킹

### Phase 3: 고도화 (2-4주)
- [ ] 웹 대시보드
- [ ] 히스토리컬 데이터 저장
- [ ] 펀딩레이트 모니터링
- [ ] 페어 추가 (추가 토큰)
- [ ] 거래소 추가 프레임워크

### Phase 4: 자동화 (선택)
- [ ] 자동 매매 모듈
- [ ] 가스비 모니터링
- [ ] 슬리피지 계산

---

## 12. 설정 파일 예시

```yaml
# config.yaml
exchanges:
  hyperliquid:
    enabled: true
    ws_url: "wss://api.hyperliquid.xyz/ws"
    rest_url: "https://api.hyperliquid.xyz"
  lighter:
    enabled: true
    ws_url: "wss://mainnet.zklighter.elliot.ai/stream"
    rest_url: "https://mainnet.zklighter.elliot.ai"
  paradex:
    enabled: true
    ws_url: "wss://ws.api.prod.paradex.trade/v1/"
    rest_url: "https://api.prod.paradex.trade"
  01xyz:
    enabled: false  # 베타 — 메인넷 출시 후 활성화
    rest_url: "https://zo-mainnet.n1.xyz"
  nado:
    enabled: true
    ws_url: "wss://gateway.prod.nado.xyz/v1/subscribe"
    rest_url: "https://gateway.prod.nado.xyz/v1"
  extended:
    enabled: true
    ws_url: "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
    rest_url: "https://api.starknet.extended.exchange/api/v1"
  variational:
    enabled: true
    rest_url: "https://omni-client-api.prod.ap-northeast-1.variational.io"
    poll_interval_ms: 1000  # REST 폴링 (WS 미지원)

pairs:
  - BTC-PERP
  - ETH-PERP
  - HYPE-PERP
  - SOL-PERP
  - BNB-PERP

thresholds:
  default:
    min_spread_percent: 0.3
    cooldown_ms: 30000
    sustained_ms: 2000
  BTC-PERP:
    min_spread_percent: 0.2  # BTC는 더 작은 차이도 의미 있음

alerts:
  telegram:
    enabled: true
    bot_token: ${TELEGRAM_BOT_TOKEN}
    chat_id: ${TELEGRAM_CHAT_ID}
  discord:
    enabled: false
```

---

## 13. 거래소별 WebSocket 구독 예시

### Hyperliquid — AllMids (전체 자산 mid price 실시간)
```json
{ "method": "subscribe", "subscription": { "type": "allMids" } }
```
응답: `{ "mids": { "BTC": "67234.5", "ETH": "3456.7", ... } }`

### Paradex — BBO (Best Bid/Offer, 50ms)
```json
{ "jsonrpc": "2.0", "method": "subscribe", "params": { "channel": "bbo.BTC-USD-PERP" } }
```

### Nado — best_bid_offer (event-driven)
```json
{ "method": "subscribe", "params": { "channel": "best_bid_offer", "market": "BTC-PERP" } }
```
- 헤더 필수: `Sec-WebSocket-Extensions: permessage-deflate`

### Lighter — Order Book (50ms 배치)
```
wss://mainnet.zklighter.elliot.ai/stream 접속 후 orderbook 채널 구독
```

### Extended — Order Book Stream (100ms)
```
GET /stream.extended.exchange/v1/orderbooks/{market}
```

---

## 14. 참고 API 문서 링크

| 거래소 | 메인 문서 | WebSocket | REST | SDK |
|--------|-----------|-----------|------|-----|
| Hyperliquid | [API Docs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api) | [WS](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket) | [Info Endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals) | [Python SDK](https://github.com/hyperliquid-dex/hyperliquid-python-sdk) |
| Paradex | [Docs](https://docs.paradex.trade) | [WS](https://docs.paradex.trade/ws/general-information/introduction) | [REST](https://docs.paradex.trade/api/general-information) | [Python SDK](https://tradeparadex.github.io/paradex-py/) |
| Nado | [Docs](https://docs.nado.xyz) | [Subscriptions](https://docs.nado.xyz/developer-resources/api/subscriptions) | [Endpoints](https://docs.nado.xyz/developer-resources/api/endpoints) | TS/Py/Rust SDK |
| Lighter | [Docs](https://docs.lighter.xyz) | [WS](https://apidocs.lighter.xyz/docs/websocket-reference) | [API](https://apidocs.lighter.xyz) | JS SDK |
| Extended | [Docs](https://docs.extended.exchange) | - | [API](https://api.docs.extended.exchange) | Python SDK |
| 01.xyz | [Docs](https://docs.01.xyz) | 미문서화 | [API](https://api.01.xyz) | `@n1xyz/nord-ts` |
| Variational | [Docs](https://docs.variational.io) | 없음 | [API](https://docs.variational.io/technical-documentation/api) | - |
