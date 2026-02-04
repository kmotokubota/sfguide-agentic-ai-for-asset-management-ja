# 実データ統合分析

## エグゼクティブサマリー

このドキュメントは`SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE`から利用可能な実データを分析し、SAMデモの合成データとの置換マッピングを示します。

### 主要な発見

| 指標 | 値 |
|------|-----|
| 総DIM_ISSUERレコード | 69,403 |
| CIK付き発行体 | 7,844 (11.3%) |
| SECメトリクスデータ付き発行体 | 4,436 |
| SEC提出書類テキスト付き発行体 | 5,233 |
| 株価データ付き証券 | 11,321 |

## 実装ステータス

### 新規作成MARKET_DATAテーブル（実データ）

| テーブル | レコード | 企業/証券 | ソース |
|---------|---------|-----------|--------|
| FACT_FINANCIAL_DATA_SEC | 100,000+ | 39社以上 | SEC_METRICS_TIMESERIES |
| FACT_STOCK_PRICES | 500,000+ | 865ティッカー以上 | STOCK_PRICE_TIMESERIES |
| FACT_SEC_FILING_TEXT | 50,000+ | 50社以上 | SEC_REPORT_TEXT_ATTRIBUTES |

### 設定

実データ統合は`python/config.py`の`config.REAL_DATA_SOURCES['enabled']`で制御されます。

有効化すると、ビルドプロセスは：
1. CIKリンケージを持つ企業の実SEC財務データのロードを試行
2. マッチするティッカーを持つ証券の実株価をロード
3. 実SEC提出書類テキスト（MD&A、リスク要因など）をロード
4. 実データが利用不可の場合は合成データにフォールバック

## 利用可能な実データソース

### 1. 株価データ (`STOCK_PRICE_TIMESERIES`)

**説明**: Nasdaqで取引される米国証券の日次始値/終値、高値/安値、取引量。

**カバレッジ**: 25,638証券に価格データあり（16,123ユニークティッカー）

**利用可能な変数**:
- Nasdaq出来高
- 日中高値 / 日中高値調整後
- 日中安値 / 日中安値調整後
- プレマーケット始値 / プレマーケット始値調整後
- アフターマーケット終値 / アフターマーケット終値調整後

**置換対象**: `CURATED.FACT_MARKETDATA_TIMESERIES`（合成OHLCVデータ）

---

### 2. SEC財務メトリクス (`SEC_METRICS_TIMESERIES`)

**説明**: 標準化されたXBRL変数を含む10-Qおよび10-Kからの四半期および年次解析収益セグメント。

**カバレッジ**: 4,436発行体に財務メトリクスデータあり

**サンプルメトリクス**:
- NET SALES | SEGMENT: IPHONE
- NET SALES | SEGMENT: SERVICE
- 地域別収益（アメリカ、ヨーロッパ、中華圏など）

**置換対象**: 
- `CURATED.FACT_FUNDAMENTALS`（合成ファンダメンタルズ）
- `MARKET_DATA.FACT_FINANCIAL_DATA`（合成財務データ）

---

### 3. SEC提出書類テキスト (`SEC_REPORT_TEXT_ATTRIBUTES`)

**説明**: SECに提出された企業提出書類（10-K、10-Q、8-K）の全文。

**カバレッジ**: 5,233発行体に提出書類テキストデータあり

**利用可能な提出書類タイプ**:
- 10-K提出書類テキスト
- 10-Q提出書類テキスト
- 8-K提出書類テキスト

**置換対象**: `MARKET_DATA.FACT_FILING_DATA`（合成提出書類テキスト）

---

### 4. 企業イベント議事録 (`COMPANY_EVENT_TRANSCRIPT_ATTRIBUTES`)

**説明**: スピーカー帰属付きJSON形式の企業イベント議事録（決算説明会、株主総会、M&A発表、投資家デー）。

**カバレッジ**: 9,000社以上の公開企業、約31デモティッカー処理済み

**処理パイプライン**:
1. **スピーカー識別** via `AI_COMPLETE` (claude-haiku-4-5)
2. **セグメントフォーマット**: "Speaker Name (Role - Company): Text"
3. **チャンキング** via `SPLIT_TEXT_RECURSIVE_CHARACTER` (~512トークン/チャンク)
4. **証券リンケージ**: PRIMARY_TICKERを介してDIM_SECURITYに結合

**新規テーブル**:
- `MARKET_DATA.COMP_EVENT_SPEAKER_MAPPING` - スピーカーID→名前/役職/企業マッピング
- `CURATED.COMPANY_EVENT_TRANSCRIPTS_CORPUS` - チャンク化された議事録コーパス

**新規Cortex Searchサービス**:
- `SAM_COMPANY_EVENTS` - フィルタリング用EVENT_TYPE属性付き検索

**置換対象**: `CURATED.EARNINGS_TRANSCRIPTS_CORPUS`（テンプレートハイドレーションからの合成）

---

## スキーマ統合計画

### CURATEDから削除するテーブル（MARKET_DATAに移動）

| CURATEDテーブル | MARKET_DATA置換 | 実データソース |
|----------------|-----------------|---------------|
| FACT_FUNDAMENTALS | FACT_FINANCIAL_DATA_SEC | SEC_METRICS_TIMESERIES |
| FACT_ESTIMATES | FACT_ESTIMATE_DATA | 合成維持（実ソースなし） |
| FACT_MARKETDATA_TIMESERIES | FACT_PRICE_HISTORY | STOCK_PRICE_TIMESERIES |
| FACT_ESG_SCORES | FACT_ESG_DATA | 合成維持（実ソースなし） |
| FACT_FACTOR_EXPOSURES | FACT_FACTOR_DATA | 合成維持（実ソースなし） |

### 合成維持するテーブル

| テーブル | 理由 |
|---------|------|
| FACT_ESG_SCORES | 公開データセットに実ESGデータなし |
| FACT_FACTOR_EXPOSURES | 公開データセットに実ファクターデータなし |
| FACT_ESTIMATE_DATA | 公開データセットに実アナリスト予想なし |
| すべて*_CORPUSテーブル | Cortex Search用ドキュメントコンテンツ |

## 実装優先順位

### フェーズ1: 高価値、高カバレッジ
1. **FACT_PRICE_HISTORY** from STOCK_PRICE_TIMESERIES
2. **FACT_FINANCIAL_DATA_SEC** from SEC_METRICS_TIMESERIES
3. **FACT_SEC_FILING_TEXT** from SEC_REPORT_TEXT_ATTRIBUTES

### フェーズ2: 強化された分析
4. **FACT_SEGMENT_FINANCIALS** - セグメント/地域別収益
5. **FACT_INSTITUTIONAL_HOLDINGS** - 13F所有権データ

### フェーズ3: クリーンアップ
6. 非推奨CURATEDテーブル削除
7. セマンティックビューをMARKET_DATA使用に更新
8. Cortex Searchサービス更新

## CIKリンケージパターン

すべての実データはCIKを介してモデルに結合：

```sql
-- 例: SECメトリクスを発行体に結合
SELECT 
    di.IssuerID,
    di.LegalName,
    smt.VARIABLE_NAME,
    smt.VALUE,
    smt.FISCAL_YEAR,
    smt.FISCAL_PERIOD
FROM SAM_DEMO.CURATED.DIM_ISSUER di
INNER JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.SEC_METRICS_TIMESERIES smt 
    ON di.CIK = smt.CIK
WHERE di.CIK IS NOT NULL;
```

## デモ企業カバレッジ

デモ企業（AAPL、MSFT、NVDAなど）はすべてCIKと完全なSECデータカバレッジを持っています：

| 企業 | CIK | SECメトリクス | 提出書類テキスト | 株価 |
|------|-----|--------------|-----------------|------|
| Apple Inc. | 0000320193 | ✅ | ✅ | ✅ |
| Microsoft Corp | 0000789019 | ✅ | ✅ | ✅ |
| NVIDIA Corp | 0001045810 | ✅ | ✅ | ✅ |
| Tesla Inc. | 0001318605 | ✅ | ✅ | ✅ |
| Visa Inc. | 0001403161 | ✅ | ✅ | ✅ |

## 実装ステータス

### ✅ 完了

1. **ETL関数作成** (`generate_market_data.py`)
2. **設定統合** (`config.py`)
3. **新規MARKET_DATAテーブル**
4. **セマンティックビュー作成**
5. **Cortex Searchサービス**
6. **ビルドプロセス更新**
7. **エージェント統合** ✅
8. **実企業イベント議事録** ✅
9. **スキーマクリーンアップ** ✅

## サマリー

SAMデモは本物のSECデータ統合を含むようになりました：

- **520万以上の実株価** Nasdaqから（OHLCVデータ）
- **9,400以上の実SEC財務メトリクス** 10-K/10-Q提出書類から
- **6,300以上の実SEC提出書類テキスト** セクション（MD&A、リスク要因など）
- **500以上の実企業イベント議事録チャンク**
- **約50社** CIKリンケージで実データクエリ可能
- **約31社** 実議事録カバレッジ
- **5エージェント更新** 実データツール付き
- **2新規セマンティックビュー**
- **2新規Cortex Searchサービス**
- **議事録のスピーカー帰属**（AI_COMPLETE経由の名前、役職、企業）
