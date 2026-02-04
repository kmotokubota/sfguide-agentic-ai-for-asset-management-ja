# SAMデモ - データリネージと依存関係

SAMデモ環境のデータフロー、オブジェクト依存関係、インパクト分析。

## 概要

このドキュメントはSAMデモのオブジェクト間の依存関係をマッピングし、変更の影響を理解するためのものです。コンポーネントを変更する際に、下流への影響を理解するためにこのガイドを使用してください。

## ビルド順序

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SAMデモビルドパイプライン                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. データベース構造                                                          │
│     └── main.py                                                              │
│         └── generate_structured.create_database_structure()                  │
│             ├── SAM_DEMO.RAW (スキーマ)                                       │
│             ├── SAM_DEMO.CURATED (スキーマ)                                   │
│             ├── SAM_DEMO.MARKET_DATA (スキーマ)                               │
│             └── SAM_DEMO.AI (スキーマ)                                        │
│                                                                              │
│  2. 構造化データ (generate_structured.py)                                     │
│     └── 基盤テーブル (依存関係順)                                              │
│         ├── DIM_ISSUER (DEMO_COMPANIES設定から)                              │
│         ├── DIM_SECURITY                                                     │
│         ├── DIM_PORTFOLIO                                                    │
│         ├── DIM_BENCHMARK                                                    │
│         ├── DIM_SUPPLY_CHAIN_RELATIONSHIPS                                   │
│         ├── FACT_TRANSACTION                                                 │
│         ├── FACT_POSITION_DAILY_ABOR                                         │
│         ├── FACT_ESG_SCORES                                                  │
│         ├── FACT_FACTOR_EXPOSURES                                            │
│         ├── FACT_BENCHMARK_HOLDINGS                                          │
│         ├── 実装テーブル (取引コスト、流動性、リスク)                           │
│         ├── クライアント/エグゼクティブテーブル (DIM_CLIENT、フロー、パフォーマンス) │
│         └── ミドルオフィステーブル (決済、照合、NAV、キャッシュ)                │
│                                                                              │
│  3. 市場データ (generate_market_data.py)                                      │
│     ├── 参照テーブル (常に構築)                                               │
│     │   ├── DIM_ANALYST                                                      │
│     │   ├── DIM_BROKER                                                       │
│     │   └── (CURATED.DIM_ISSUERを企業マスターとして使用 - DIM_COMPANYなし)    │
│     ├── 実データ (REAL_DATA_SOURCES有効時はプライマリ)                        │
│     │   ├── FACT_SEC_FINANCIALS (TAM、顧客数、NRR付き)                       │
│     │   ├── FACT_STOCK_PRICES                                                │
│     │   ├── FACT_FINANCIAL_DATA_SEC                                          │
│     │   └── FACT_SEC_FILING_TEXT                                             │
│     └── 派生データ (FACT_SEC_FINANCIALSから)                                  │
│         ├── FACT_ESTIMATE_CONSENSUS (実際のSEC実績を使用)                     │
│         └── FACT_ESTIMATE_DATA                                               │
│                                                                              │
│  4. 非構造化データ (generate_unstructured.py)                                 │
│     └── ドキュメントタイプ (並列)                                             │
│         ├── RAW.*_RAWテーブル ──► CURATED.*_CORPUSテーブル                   │
│         │   ├── BROKER_RESEARCH_CORPUS                                       │
│         │   ├── PRESS_RELEASES_CORPUS                                        │
│         │   ├── NGO_REPORTS_CORPUS                                           │
│         │   ├── ENGAGEMENT_NOTES_CORPUS                                      │
│         │   ├── POLICY_DOCS_CORPUS                                           │
│         │   ├── SALES_TEMPLATES_CORPUS                                       │
│         │   ├── PHILOSOPHY_DOCS_CORPUS                                       │
│         │   ├── REPORT_TEMPLATES_CORPUS                                      │
│         │   ├── MACRO_EVENTS_CORPUS                                          │
│         │   ├── CUSTODIAN_REPORTS_CORPUS                                     │
│         │   ├── RECONCILIATION_NOTES_CORPUS                                  │
│         │   ├── SSI_DOCUMENTS_CORPUS                                         │
│         │   ├── OPS_PROCEDURES_CORPUS                                        │
│         │   └── STRATEGY_DOCUMENTS_CORPUS                                    │
│         └── 実トランスクリプト (generate_real_transcripts.py)                 │
│             └── COMPANY_EVENT_TRANSCRIPTS_CORPUS                             │
│                                                                              │
│  5. AIコンポーネント (build_ai.py)                                            │
│     ├── セマンティックビュー (create_semantic_views.py)                       │
│     │   ├── SAM_ANALYST_VIEW (ファクターエクスポージャーとベンチマーク含む)    │
│     │   ├── SAM_FUNDAMENTALS_VIEW                                            │
│     │   ├── SAM_IMPLEMENTATION_VIEW                                          │
│     │   ├── SAM_SUPPLY_CHAIN_VIEW                                            │
│     │   ├── SAM_MIDDLE_OFFICE_VIEW (コーポレートアクション、キャッシュ含む)    │
│     │   ├── SAM_COMPLIANCE_VIEW                                              │
│     │   ├── SAM_EXECUTIVE_VIEW                                               │
│     │   ├── SAM_STOCK_PRICES_VIEW                                            │
│     │   └── SAM_SEC_FINANCIALS_VIEW                                          │
│     ├── Cortex Searchサービス (create_cortex_search.py)                       │
│     │   ├── SAM_BROKER_RESEARCH                                              │
│     │   ├── SAM_COMPANY_EVENTS                                               │
│     │   ├── SAM_PRESS_RELEASES                                               │
│     │   ├── SAM_NGO_REPORTS                                                  │
│     │   ├── SAM_ENGAGEMENT_NOTES                                             │
│     │   ├── SAM_POLICY_DOCS                                                  │
│     │   ├── SAM_SALES_TEMPLATES                                              │
│     │   ├── SAM_PHILOSOPHY_DOCS                                              │
│     │   ├── SAM_REPORT_TEMPLATES                                             │
│     │   ├── SAM_MACRO_EVENTS                                                 │
│     │   ├── SAM_CUSTODIAN_REPORTS                                            │
│     │   ├── SAM_RECONCILIATION_NOTES                                         │
│     │   ├── SAM_SSI_DOCUMENTS                                                │
│     │   ├── SAM_OPS_PROCEDURES                                               │
│     │   ├── SAM_STRATEGY_DOCUMENTS                                           │
│     │   └── SAM_REAL_SEC_FILINGS                                             │
│     └── エージェント (create_agents.py)                                       │
│         ├── AM_portfolio_copilot                                             │
│         ├── AM_research_copilot                                              │
│         ├── AM_thematic_macro_advisor                                        │
│         ├── AM_esg_guardian                                                  │
│         ├── AM_compliance_advisor                                            │
│         ├── AM_sales_advisor                                                 │
│         ├── AM_quant_analyst                                                 │
│         ├── AM_middle_office_copilot                                         │
│         └── AM_executive_copilot                                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 外部データソースマッピング

### SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREEからSAM_DEMOへのマッピング

| ソーステーブル | ターゲットテーブル | 説明 | 結合キー |
|---------------|-------------------|------|---------|
| `STOCK_PRICE_TIMESERIES` | `MARKET_DATA.FACT_STOCK_PRICES` | Nasdaqからの日次OHLCV株価 | TICKER |
| `SEC_METRICS_TIMESERIES` | `MARKET_DATA.FACT_SEC_SEGMENTS` | 地域、事業部門、顧客、法人別収益セグメント | COMPANY_ID → ProviderCompanyID |
| `SEC_REPORT_TEXT_ATTRIBUTES` | `MARKET_DATA.FACT_SEC_FILING_TEXT` | SEC提出書類全文（MD&A、リスク要因） | CIK |
| `SEC_CORPORATE_REPORT_ATTRIBUTES` | `MARKET_DATA.FACT_SEC_FINANCIALS` | XBRLタグ付き完全財務諸表 | CIK |
| `COMPANY_EVENT_TRANSCRIPT_ATTRIBUTES` | `CURATED.COMPANY_EVENT_TRANSCRIPTS_CORPUS` | スピーカー帰属付き実企業イベント議事録 | PRIMARY_TICKER |

### ソースからターゲットへのカラムマッピング

#### STOCK_PRICE_TIMESERIESからFACT_STOCK_PRICESへ

| ソースカラム | ターゲットカラム | 変換 |
|-------------|-----------------|------|
| `TICKER` | `TICKER` | 直接マッピング |
| `DATE` | `PRICE_DATE` | リネーム |
| `VARIABLE='pre-market_open'` | `PRICE_OPEN` | VARIABLEでピボット |
| `VARIABLE='post-market_close'` | `PRICE_CLOSE` | VARIABLEでピボット |
| `VARIABLE='all-day_high'` | `PRICE_HIGH` | VARIABLEでピボット |
| `VARIABLE='all-day_low'` | `PRICE_LOW` | VARIABLEでピボット |
| `VARIABLE='nasdaq_volume'` | `VOLUME` | VARIABLEでピボット、BIGINTにキャスト |
| - | `SecurityID` | TICKERを介してDIM_SECURITYから結合 |
| - | `IssuerID` | TICKERを介してDIM_SECURITYから結合 |

### 適用されるデータフィルタリング

| ソーステーブル | フィルタ条件 | 理由 |
|---------------|-------------|------|
| `STOCK_PRICE_TIMESERIES` | `DATE >= DATEADD(year, -2, CURRENT_DATE())` | 過去2年間の価格 |
| `SEC_METRICS_TIMESERIES` | `FISCAL_YEAR >= YEAR(CURRENT_DATE()) - 5` | 過去5年間の提出書類 |
| `SEC_METRICS_TIMESERIES` | `CIK IS NOT NULL AND VALUE IS NOT NULL` | 有効なレコードのみ |
| `SEC_REPORT_TEXT_ATTRIBUTES` | `LENGTH(VALUE) > 100` | 空/短いテキストをフィルタ |
| `SEC_REPORT_TEXT_ATTRIBUTES` | `PERIOD_END_DATE >= DATEADD(year, -3, CURRENT_DATE())` | 過去3年間の提出書類 |

## 依存関係グラフ

### CURATEDスキーマの依存関係

```
┌───────────────────────────────────────────────────────────────────┐
│                    CURATEDスキーマの依存関係                        │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  config.DEMO_COMPANIES (真実の源泉)                                │
│  │                                                                │
│  └──► DIM_ISSUER                                                  │
│       ├── CIK (SECデータにリンク)                                 │
│       ├── IssuerID (PK)                                          │
│       │                                                           │
│       └──► DIM_SECURITY                                          │
│            ├── IssuerID (DIM_ISSUERへのFK)                       │
│            ├── SecurityID (PK)                                    │
│            │                                                      │
│            ├──► FACT_TRANSACTION                                 │
│            │    ├── SecurityID (FK)                              │
│            │    ├── PortfolioID (DIM_PORTFOLIOへのFK)            │
│            │    │                                                 │
│            │    └──► FACT_POSITION_DAILY_ABOR                    │
│            │         ├── FACT_TRANSACTIONから派生                 │
│            │         └──► V_HOLDINGS_WITH_ESG (ビュー)           │
│            │                                                      │
│            ├──► FACT_ESG_SCORES                                  │
│            │    └── SecurityID (FK)                              │
│            │                                                      │
│            ├──► FACT_FACTOR_EXPOSURES                            │
│            │    └── SecurityID (FK)                              │
│            │                                                      │
│            └──► 実装/オペレーションテーブル                        │
│                 ├── FACT_TRANSACTION_COSTS                       │
│                 ├── FACT_TRADING_CALENDAR                        │
│                 ├── FACT_TAX_IMPLICATIONS                        │
│                 └── ミドルオフィステーブル                         │
│                                                                   │
│  DIM_SUPPLY_CHAIN_RELATIONSHIPS                                   │
│  └── DIM_ISSUERからIssuerIDを使用                                │
│                                                                   │
│  DIM_PORTFOLIO (独立)                                             │
│  └── PortfolioID (PK)                                            │
│       ├──► FACT_TRANSACTION                                      │
│       ├──► FACT_POSITION_DAILY_ABOR                              │
│       ├──► FACT_PORTFOLIO_LIQUIDITY                              │
│       ├──► FACT_RISK_LIMITS                                      │
│       └──► DIM_CLIENT_MANDATES                                   │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

## インパクト分析マトリックス

### 変更した場合... 必要な対応...

| 変更オブジェクト | 直接的な影響 | 再構築/更新が必要 |
|-----------------|-------------|-------------------|
| **config.DEMO_COMPANIES** | すべてのデータ生成が変更 | フル再構築推奨 |
| **DIM_ISSUER** | すべてのFK参照が壊れる | DIM_SECURITY、DIM_SUPPLY_CHAIN、IssuerID付きすべてのMARKET_DATAテーブル、すべてのコーパステーブル、すべてのセマンティックビュー |
| **DIM_SECURITY** | 保有銘柄、取引が壊れる | FACT_TRANSACTION、FACT_POSITION_DAILY_ABOR、FACT_ESG_SCORES、FACT_FACTOR_EXPOSURES、FACT_STOCK_PRICES、すべてのコーパステーブル |
| **DIM_PORTFOLIO** | 保有銘柄、取引が壊れる | FACT_TRANSACTION、FACT_POSITION_DAILY_ABOR、クライアント/エグゼクティブテーブル |
| **FACT_POSITION_DAILY_ABOR** | ポートフォリオ分析が壊れる | V_HOLDINGS_WITH_ESG、SAM_ANALYST_VIEW、portfolio_copilot |
| **FACT_STOCK_PRICES** | 価格分析が壊れる | SAM_STOCK_PRICES_VIEW、V_SECURITY_RETURNS、stock_pricesツール使用エージェント |
| **FACT_SEC_FINANCIALS** | 予想生成とファンダメンタルズビューが壊れる | FACT_ESTIMATE_CONSENSUS、SAM_FUNDAMENTALS_VIEW |
| ***_CORPUSテーブル** | ドキュメント検索が壊れる | 対応するSAM_* Cortex Searchサービス |
| **SAM_ANALYST_VIEW** | ポートフォリオクエリが壊れる | portfolio_copilot、quant_analyst、thematic_macro_advisor、esg_guardian |
| **content_libraryテンプレート** | ドキュメント品質が変更 | コーパステーブル再生成、検索サービス再構築 |

## 再構築手順

### フル再構築
```bash
python main.py --connection-name CONNECTION --scenarios all
```
正しい依存関係順序ですべてを再構築します。

### 部分再構築

| スコープ | コマンド | 再構築対象 |
|---------|---------|-----------|
| 構造化のみ | `--scope structured` | CURATEDディメンション/ファクトテーブル |
| 非構造化のみ | `--scope unstructured` | RAW/*_RAW、CURATED/*_CORPUS |
| 市場データのみ | (structuredに含まれる) | MARKET_DATAテーブル |
| AIのみ | `--scope ai` | セマンティックビュー、検索サービス、エージェント |
| セマンティックビューのみ | `--scope semantic` | AIセマンティックビュー |
| 検索サービスのみ | `--scope search` | AI Cortex Searchサービス |
| エージェントのみ | `--scope agents` | AIエージェント |

## CIKリンケージマップ

CIK（SEC中央インデックスキー）は内部データを実際のSECデータにリンクします：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CIKリンケージマップ                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE                                 │
│  (config.REAL_DATA_SOURCES経由)                                              │
│                                                                              │
│  COMPANY_INDEX.CIK ─────────────────────────────────────────────┐           │
│  ├── SEC_METRICS_TIMESERIES.CIK ──────────────────────────────────┤         │
│  ├── SEC_REPORT_TEXT_ATTRIBUTES.CIK ──────────────────────────────┤         │
│  ├── SEC_CORPORATE_REPORT_ATTRIBUTES.CIK ─────────────────────────┤         │
│  └── STOCK_PRICE_TIMESERIES (ティッカー経由、CIKではない) ──────────┤         │
│                                                                    │         │
│                                                                    ▼         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  CURATED.DIM_ISSUER.CIK (単一の真実の源泉)                          │    │
│  │  │                                                                   │    │
│  │  ├──► FACT_FINANCIAL_DATA_SEC.CIK (直接結合)                       │    │
│  │  ├──► FACT_SEC_FILING_TEXT.IssuerID                                │    │
│  │  ├──► FACT_SEC_FINANCIALS.IssuerID                                 │    │
│  │  │                                                                   │    │
│  │  └──► 以下の間の結合を可能に:                                       │    │
│  │       - 内部保有銘柄 (DIM_SECURITY → DIM_ISSUER)                   │    │
│  │       - 実際のSEC財務データ (FACT_SEC_FINANCIALS)                  │    │
│  │       - 実際のSEC提出書類 (FACT_SEC_FILING_TEXT)                   │    │
│  │       - 実際の株価 (ティッカー経由のFACT_STOCK_PRICES)              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  カバレッジ: 79社すべてがCIKリンケージを持つ (100% SECデータカバレッジ)       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## トラブルシューティング依存関係の問題

### よくあるエラー

| エラー | 考えられる原因 | 解決策 |
|-------|--------------|--------|
| セマンティックビューで"Object does not exist" | 基礎テーブルが作成されていない | `--scope structured`で再構築 |
| セマンティックビューで"Invalid identifier" | カラム名の不一致 | `DESCRIBE TABLE`で実際のカラム名を確認 |
| 検索サービスから"No results" | コーパステーブルが空 | `--scope unstructured`で再構築 |
| "Agent tool not found" | セマンティックビューまたは検索サービスが不足 | `--scope ai`で再構築 |
| "CIK linkage returns 0 records" | DIM_ISSUERにCIKがない | DEMO_COMPANIES設定を確認 |

### 検証クエリ

```sql
-- CIKカバレッジを確認
SELECT COUNT(*) as total, COUNT(CIK) as with_cik FROM SAM_DEMO.CURATED.DIM_ISSUER;

-- 実データ統合を確認
SELECT COUNT(*) FROM SAM_DEMO.MARKET_DATA.FACT_STOCK_PRICES;
SELECT COUNT(*) FROM SAM_DEMO.MARKET_DATA.FACT_FINANCIAL_DATA_SEC;
SELECT COUNT(*) FROM SAM_DEMO.MARKET_DATA.FACT_SEC_FILING_TEXT;

-- セマンティックビューを確認
SHOW SEMANTIC VIEWS IN SAM_DEMO.AI;

-- 検索サービスを確認
SHOW CORTEX SEARCH SERVICES IN SAM_DEMO.AI;

-- エージェントを確認
SHOW AGENTS IN SAM_DEMO.AI;
```
