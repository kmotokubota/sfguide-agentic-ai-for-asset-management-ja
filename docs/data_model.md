# SAMデモ - データモデルドキュメント

Simulated Asset Management（SAM）デモのデータアーキテクチャドキュメント。`DEMO_COMPANIES`設定から79社の実在企業を使用し、業界標準のアセットマネジメントデータプラクティスに準拠。すべての企業はCIK識別子を持ち、完全なSECデータ統合が可能です。

## データベースアーキテクチャ

**データベース**: `SAM_DEMO`

**スキーマ**:
- **RAW**: プロバイダーシミュレーションと生の非構造化ドキュメント
- **CURATED**: 分析用の業界標準ディメンション/ファクトモデル
- **MARKET_DATA**: SNOWFLAKE_PUBLIC_DATA_FREEからの実際の市場データ
- **AI**: セマンティックビューとCortex Searchサービス

## CURATEDスキーマ

### ディメンションテーブル

| テーブル | 説明 |
|---------|------|
| `DIM_ISSUER` | DEMO_COMPANIESからの79社の実在発行体、すべてSECデータリンケージ用CIK識別子付き |
| `DIM_SECURITY` | DIM_ISSUERから派生したティッカー識別子付き証券（発行体と1:1） |
| `DIM_PORTFOLIO` | 戦略、通貨、設定日を含む10のポートフォリオ |
| `DIM_BENCHMARK` | 3つのベンチマーク: S&P 500、MSCI ACWI、Nasdaq 100 |
| `DIM_SUPPLY_CHAIN_RELATIONSHIPS` | 発行体間のサプライチェーン関係 |
| `DIM_CLIENT` | フローパターン指標を含むクライアントディメンション（フロー減少中のリスククライアント、オンボーディング追跡付き新規クライアント） |
| `DIM_CLIENT_MANDATES` | クライアントマンデートの制約と要件 |
| `DIM_COUNTERPARTY` | 取引カウンターパーティ参照データ |
| `DIM_CUSTODIAN` | カストディアン参照データ |

### コアファクトテーブル

| テーブル | 説明 |
|---------|------|
| `FACT_TRANSACTION` | 12ヶ月履歴を持つ正規取引ログ |
| `FACT_POSITION_DAILY_ABOR` | 取引から派生した日次ABORポジション |
| `FACT_ESG_SCORES` | セクター差別化を含む月次ESG格付け |
| `FACT_FACTOR_EXPOSURES` | 月次ファクタースコア（バリュー、グロース、クオリティなど） |
| `FACT_BENCHMARK_HOLDINGS` | ベンチマーク構成銘柄ポジション |
| `FACT_BENCHMARK_PERFORMANCE` | ポートフォリオ対ベンチマーク比較用のベンチマークレベルリターン（MTD、QTD、YTD） |

### 実装計画テーブル

| テーブル | 説明 |
|---------|------|
| `FACT_TRANSACTION_COSTS` | 取引コスト、ビッドアスクスプレッド、市場インパクトデータ |
| `FACT_PORTFOLIO_LIQUIDITY` | キャッシュポジション、キャッシュフロー、流動性スコア |
| `FACT_RISK_LIMITS` | リスクバジェット、トラッキングエラー制限、集中制限 |
| `FACT_TRADING_CALENDAR` | 決算日、ブラックアウト期間、市場イベント |
| `FACT_TAX_IMPLICATIONS` | コストベース、未実現利益、タックスロスハーベスティング |

### エグゼクティブおよびクライアント分析テーブル

| テーブル | 説明 |
|---------|------|
| `FACT_CLIENT_FLOWS` | 差別化されたパターンを持つクライアントの申込・解約フロー（リスククライアントは減少/解約傾向、新規クライアントは短いフロー履歴） |
| `FACT_FUND_FLOWS` | ファンドレベルの集約フローデータ |
| `FACT_STRATEGY_PERFORMANCE` | 戦略レベルのパフォーマンス指標 |
| `FACT_COMPLIANCE_ALERTS` | コンプライアンスアラート履歴 |
| `FACT_PRE_SCREENED_REPLACEMENTS` | 事前承認済み証券代替 |

### ミドルオフィスオペレーションテーブル

| テーブル | 説明 |
|---------|------|
| `FACT_TRADE_SETTLEMENT` | 取引決済ステータスと履歴 |
| `FACT_RECONCILIATION` | ポジションおよびキャッシュ照合データ |
| `FACT_NAV_CALCULATION` | NAV計算結果 |
| `FACT_NAV_COMPONENTS` | NAVコンポーネント内訳 |
| `FACT_CORPORATE_ACTIONS` | コーポレートアクションイベント |
| `FACT_CORPORATE_ACTION_IMPACT` | コーポレートアクションのポートフォリオインパクト |
| `FACT_CASH_MOVEMENTS` | キャッシュムーブメント取引 |
| `FACT_CASH_POSITIONS` | 日次キャッシュポジションスナップショット |

### ビュー

| ビュー | 説明 |
|--------|------|
| `V_HOLDINGS_WITH_ESG` | 最新ESGスコアで強化された保有銘柄 |
| `V_SECURITY_RETURNS` | 価格データから計算されたリターンを持つ証券 |

### ドキュメントコーパステーブル

| コーパステーブル | 検索サービス | リンケージレベル |
|-----------------|--------------|-----------------|
| `BROKER_RESEARCH_CORPUS` | `SAM_BROKER_RESEARCH` | 証券 |
| `COMPANY_EVENT_TRANSCRIPTS_CORPUS` | `SAM_COMPANY_EVENTS` | 証券（実データ） |
| `PRESS_RELEASES_CORPUS` | `SAM_PRESS_RELEASES` | 証券 |
| `NGO_REPORTS_CORPUS` | `SAM_NGO_REPORTS` | 発行体 |
| `ENGAGEMENT_NOTES_CORPUS` | `SAM_ENGAGEMENT_NOTES` | 発行体 |
| `POLICY_DOCS_CORPUS` | `SAM_POLICY_DOCS` | グローバル |
| `SALES_TEMPLATES_CORPUS` | `SAM_SALES_TEMPLATES` | グローバル |
| `PHILOSOPHY_DOCS_CORPUS` | `SAM_PHILOSOPHY_DOCS` | グローバル |
| `REPORT_TEMPLATES_CORPUS` | `SAM_REPORT_TEMPLATES` | グローバル |
| `MACRO_EVENTS_CORPUS` | `SAM_MACRO_EVENTS` | グローバル |
| `CUSTODIAN_REPORTS_CORPUS` | `SAM_CUSTODIAN_REPORTS` | ポートフォリオ |
| `RECONCILIATION_NOTES_CORPUS` | `SAM_RECONCILIATION_NOTES` | グローバル |
| `SSI_DOCUMENTS_CORPUS` | `SAM_SSI_DOCUMENTS` | グローバル |
| `OPS_PROCEDURES_CORPUS` | `SAM_OPS_PROCEDURES` | グローバル |
| `STRATEGY_DOCUMENTS_CORPUS` | `SAM_STRATEGY_DOCUMENTS` | グローバル |

## MARKET_DATAスキーマ

### 参照テーブル

| テーブル | 説明 |
|---------|------|
| `DIM_ANALYST` | ブローカーアナリストディメンション |
| `DIM_BROKER` | ブローカー/リサーチ会社ディメンション |

**注意**: 企業マスターデータは`CURATED.DIM_ISSUER`によって提供されます - 別途`DIM_COMPANY`テーブルはありません。`DIM_ISSUER`がすべての企業/発行体情報の単一の真実源です。

### 実際のSECデータ（プライマリソース）

データは`SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE`から取得。79社すべてが完全なSECデータリンケージを可能にするCIK識別子を持っています。**これは現在、財務データのプライマリソースです。**

| テーブル | ソーステーブル | 説明 |
|---------|---------------|------|
| `FACT_SEC_FINANCIALS` | `SEC_CORPORATE_REPORT_ATTRIBUTES` | **プライマリ財務データテーブル**。損益計算書、貸借対照表、キャッシュフロー、計算された比率（マージン、ROE、ROA）、ヒューリスティックに計算された投資メモメトリクス（TAM、顧客数、NRR）を含むXBRLタグ付き包括的財務諸表。`SAM_FUNDAMENTALS_VIEW`を駆動し、`FACT_ESTIMATE_CONSENSUS`のベース実績を提供。 |
| `FACT_SEC_SEGMENTS` | `SEC_METRICS_TIMESERIES` | **収益セグメント内訳**。地域別（ヨーロッパ、アメリカ、アジア太平洋）、事業セグメント別、顧客別、法人別。競合地域分析を可能に（例：ブラックロックの欧州収益約60億ドル）。地理的および部門別収益クエリ用の`SAM_SEC_FINANCIALS_VIEW`で使用。 |
| `FACT_STOCK_PRICES` | `STOCK_PRICE_TIMESERIES` | Nasdaqからの実際の日次株価（OHLCV） |
| `FACT_SEC_FILING_TEXT` | `SEC_REPORT_TEXT_ATTRIBUTES` | SEC提出書類テキスト（MD&A、リスク要因） |
| `COMP_EVENT_SPEAKER_MAPPING` | AI処理済み | 議事録からのスピーカー識別 |

### 投資メモメトリクス（実際のSECデータから計算）

以下のメトリクスは`FACT_SEC_FINANCIALS`の実際のSECデータからヒューリスティックに計算されます：

| メトリクス | 計算 | 根拠 |
|-----------|------|------|
| `TAM` | 収益 × 業界乗数（15-35倍） | 業界に基づく標準的な市場規模算出アプローチ |
| `ESTIMATED_CUSTOMER_COUNT` | 収益 / ARPC（業界により異なる） | 収益から推定される顧客ベース |
| `ESTIMATED_NRR_PCT` | 100 + 収益成長率%、90-140%で上限 | SaaSスタイルのNRRは収益成長と相関 |

### アナリストデータ

| テーブル | 説明 |
|---------|------|
| `FACT_ESTIMATE_CONSENSUS` | `FACT_SEC_FINANCIALS`の実際のSEC実績から**派生した**アナリスト予想コンセンサス |
| `FACT_ESTIMATE_DATA` | 個別アナリスト予想 |
| `FACT_ANALYST_COVERAGE` | アナリストカバレッジマッピング |

## AIスキーマ

### セマンティックビュー

| セマンティックビュー | 説明 | 主要テーブル |
|--------------------|------|-------------|
| `SAM_ANALYST_VIEW` | ファクター分析を含むポートフォリオ分析 | `V_HOLDINGS_WITH_ESG`、`DIM_PORTFOLIO`、`DIM_SECURITY`、`DIM_ISSUER`、`FACT_FACTOR_EXPOSURES`、`FACT_BENCHMARK_HOLDINGS` |
| `SAM_FUNDAMENTALS_VIEW` | 財務分析 | `FACT_SEC_FINANCIALS`、`FACT_ESTIMATE_CONSENSUS`、`DIM_ISSUER`（計算されたTAM/NRRを含む実際のSECデータを使用） |
| `SAM_IMPLEMENTATION_VIEW` | 取引実装 | `FACT_TRANSACTION_COSTS`、`FACT_PORTFOLIO_LIQUIDITY`、`FACT_RISK_LIMITS` |
| `SAM_SUPPLY_CHAIN_VIEW` | サプライチェーンリスク | `DIM_SUPPLY_CHAIN_RELATIONSHIPS`、`DIM_ISSUER` |
| `SAM_MIDDLE_OFFICE_VIEW` | オペレーション監視 | `FACT_TRADE_SETTLEMENT`、`FACT_RECONCILIATION`、`FACT_NAV_CALCULATION`、`FACT_CORPORATE_ACTIONS`、`FACT_CASH_MOVEMENTS`、`FACT_CASH_POSITIONS`、`DIM_COUNTERPARTY` |
| `SAM_COMPLIANCE_VIEW` | 違反追跡 | `FACT_COMPLIANCE_ALERTS`、`FACT_RISK_LIMITS` |
| `SAM_EXECUTIVE_VIEW` | 会社全体のKPI | `FACT_CLIENT_FLOWS`、`FACT_FUND_FLOWS`、`FACT_STRATEGY_PERFORMANCE` |
| `SAM_STOCK_PRICES_VIEW` | 実際の株価 | `FACT_STOCK_PRICES`、`DIM_SECURITY`、`DIM_ISSUER` |
| `SAM_SEC_FINANCIALS_VIEW` | 包括的SECデータ | `FACT_SEC_FINANCIALS`、`DIM_ISSUER` |

### Cortex Searchサービス

| 検索サービス | コーパステーブル | 説明 |
|-------------|-----------------|------|
| `SAM_BROKER_RESEARCH` | `BROKER_RESEARCH_CORPUS` | アナリストレポート |
| `SAM_COMPANY_EVENTS` | `COMPANY_EVENT_TRANSCRIPTS_CORPUS` | 実際の企業イベント議事録 |
| `SAM_PRESS_RELEASES` | `PRESS_RELEASES_CORPUS` | 企業発表 |
| `SAM_NGO_REPORTS` | `NGO_REPORTS_CORPUS` | ESGコントロバーシーレポート |
| `SAM_ENGAGEMENT_NOTES` | `ENGAGEMENT_NOTES_CORPUS` | 企業エンゲージメントノート |
| `SAM_POLICY_DOCS` | `POLICY_DOCS_CORPUS` | 投資ポリシー |
| `SAM_SALES_TEMPLATES` | `SALES_TEMPLATES_CORPUS` | クライアント資料 |
| `SAM_PHILOSOPHY_DOCS` | `PHILOSOPHY_DOCS_CORPUS` | 投資哲学 |
| `SAM_REPORT_TEMPLATES` | `REPORT_TEMPLATES_CORPUS` | レポートテンプレート |
| `SAM_MACRO_EVENTS` | `MACRO_EVENTS_CORPUS` | マクロイベント通知 |
| `SAM_CUSTODIAN_REPORTS` | `CUSTODIAN_REPORTS_CORPUS` | カストディアンレポート |
| `SAM_RECONCILIATION_NOTES` | `RECONCILIATION_NOTES_CORPUS` | 照合ノート |
| `SAM_SSI_DOCUMENTS` | `SSI_DOCUMENTS_CORPUS` | 決済指示 |
| `SAM_OPS_PROCEDURES` | `OPS_PROCEDURES_CORPUS` | オペレーション手順 |
| `SAM_STRATEGY_DOCUMENTS` | `STRATEGY_DOCUMENTS_CORPUS` | 戦略ドキュメント |
| `SAM_REAL_SEC_FILINGS` | `FACT_SEC_FILING_TEXT` | 実際のSEC提出書類テキスト |

### エージェント

| エージェント | 表示名 | 主要ツール |
|-------------|--------|-----------|
| `AM_portfolio_copilot` | Portfolio Co-Pilot | quantitative_analyzer、stock_prices、sec_financials、検索サービス |
| `AM_research_copilot` | Research Analyst | fundamentals_analyzer、sec_financials、検索サービス |
| `AM_thematic_macro_advisor` | Thematic Macro Advisor | quantitative_analyzer、検索サービス |
| `AM_esg_guardian` | ESG Guardian | quantitative_analyzer、search_sec_filings |
| `AM_compliance_advisor` | Compliance Advisor | コンプライアンスツール、ポリシー検索 |
| `AM_sales_advisor` | Sales Advisor | quantitative_analyzer、検索サービス |
| `AM_quant_analyst` | Quant Analyst | quantitative_analyzer、stock_prices、検索サービス |
| `AM_middle_office_copilot` | Middle Office Co-Pilot | ミドルオフィスツール、オペレーション検索 |
| `AM_executive_copilot` | Executive Co-Pilot | エグゼクティブ分析、戦略検索 |

## データ品質基準

### 検証ルール

- **ポートフォリオウェイト**: 合計100%（±0.1%許容）
- **取引整合性**: 取引ログがABORポジションと一致
- **証券識別子**: ティッカー列が適切に設定
- **価格データ**: 負の価格なし、資産クラス別の現実的な範囲
- **日付一貫性**: 営業日のみ、適切な日付範囲
- **外部キー関係**: すべての関係が有効

## サンプルデータの特性

### ポートフォリオ
- SAM Global Flagship Multi-Asset
- SAM ESG Leaders Global Equity
- SAM Global Thematic Growth
- SAM Technology & Infrastructure
- その他6つの戦略

### 企業（合計79社、すべてCIK付き）
- **コアデモ企業**（8社）: AAPL、MSFT、NVDA、GOOGL、TSM、SNOW、CMC、RBBN
- **主要米国株**（約40社）: AMZN、META、TSLA、AMD、INTC等
- **追加企業**（約31社）: セクター多様性

### カバレッジ
- 米国: 約55%
- 欧州: 約30%
- APAC/EM: 約15%

## 関連ドキュメント

- [`docs/data_lineage.md`](data_lineage.md) - データフロー、依存関係、インパクト分析
- [`.cursor/rules/data-index.mdc`](../.cursor/rules/data-index.mdc) - データ生成パターン（関連ルールへのインデックス）
