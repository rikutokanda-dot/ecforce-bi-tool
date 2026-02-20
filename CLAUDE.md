# ECforce BI Tool - セッション引き継ぎ用スキルシート

## プロジェクト概要

Streamlit製のBIダッシュボード。BigQuery（全カラムSTRING型）からデータを取得し、定期通販のコホート分析・Tier分析・解約分析・広告効果分析を行う。

## デプロイ先

| 環境 | URL | 備考 |
|---|---|---|
| ローカル | http://localhost:8501 | `streamlit run app.py --server.port 8501` |
| Streamlit Cloud | https://ecforce-bi-tool-6g2geuwojw67gcfnb279aq.streamlit.app | GitHubのmainブランチ自動デプロイ |
| Cloud Run | https://ecforce-bi-984768575090.asia-northeast1.run.app | 手動デプロイ (`gcloud run deploy`) |

### Cloud Runデプロイコマンド

```bash
cd /Users/rkt_knd/Downloads/claude/BI
gcloud run deploy ecforce-bi \
  --project=amazon-scraper-482713 \
  --region=asia-northeast1 \
  --source=. \
  --port=8080 \
  --memory=1Gi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2 \
  --allow-unauthenticated \
  --set-secrets="/app/.streamlit/secrets.toml=streamlit-secrets:latest" \
  --timeout=300 \
  --quiet
```

- GCPプロジェクト: `amazon-scraper-482713`（`ecforce-data`は権限不足）
- Secrets: Secret Managerの `streamlit-secrets` にsecrets.tomlを格納
- サービスアカウント: `984768575090-compute@developer.gserviceaccount.com`

## マスタデータ永続化 (GCS)

マスタデータはGCSバケット `gs://ecforce-bi-config/config/` に保存。Cloud Run再デプロイしても消えない。

| ファイル | 内容 |
|---|---|
| `companies.yaml` | 会社マスタ (yakuin, generic, neus, clinic) |
| `product_cycles.yaml` | 商品別発送サイクル |
| `upsell_mapping.yaml` | アップセルマッピング |
| `ad_url_mapping.yaml` | 広告URL ID→表示名マッピング |

- `config_loader.py`が**GCS優先、ローカルフォールバック**で読み書き
- Cloud Runのサービスアカウントに `roles/storage.objectAdmin` 付与済み
- ローカルからGCSに書き込む場合は `gcloud storage cp` を使う（ローカルのPython環境からはGCSに直接書けない場合がある）

## BigQueryスキーマ（全カラムSTRING型）

テーブル: `` `ecforce-data.{company_key}_ecforce_raw_data.{company_key}_all_integrated` ``

```
顧客_id                          STRING  顧客ID
受注_id                          STRING  受注ID
定期受注_作成日時                STRING  定期受注作成日時 → SAFE_CAST AS TIMESTAMP
受注_定期回数                    STRING  何回目の定期注文か → SAFE_CAST AS INT64
受注_論理連番                    STRING  1=初回, 2=再処理, NULL=失敗 → SAFE_CAST AS INT64
受注_対応状況                    STRING  shipped等
受注_決済状況                    STRING  completed等
受注_決済金額                    STRING  決済金額 → SAFE_CAST AS FLOAT64
受注_受注商品_商品名            STRING  受注商品名
定期受注_受注商品_商品名        STRING  定期商品名（主要分析軸）
定期受注_受注商品_商品カテゴリ  STRING  商品カテゴリ
受注_広告url_id                  STRING  広告URL ID（.0あり→REGEXP_REPLACEで正規化）
受注_広告url_グループ名          STRING  広告グループ
定期受注_ステータス              STRING  定期ステータス（アクティブ/キャンセル等）
定期受注_キャンセル理由名        STRING  キャンセル理由
定期受注_キャンセル日時          STRING  キャンセル日時
受注_売上日時                    STRING  売上日時
受注_定期受注id                  STRING  定期受注ID
定期受注_次回発送予定日          STRING  次回発送予定日
受注_受注商品_sku                STRING  SKU
受注_作成日時_yyyymmdd           STRING  受注作成日
受注件数                         STRING  受注件数
```

### SAFE_CASTが必須

全カラムSTRING型のため、SQL内で比較・集計時にSAFE_CASTが必須:

```sql
SAFE_CAST(`受注_定期回数` AS INT64) = 1
SAFE_CAST(`定期受注_作成日時` AS TIMESTAMP) >= '2024-01-01'
SAFE_CAST(`受注_決済金額` AS FLOAT64)
FORMAT_TIMESTAMP('%Y-%m', SAFE_CAST(`定期受注_作成日時` AS TIMESTAMP))
```

`cohort.py`ではモジュールレベル定数で定義:
```python
_TS = f"SAFE_CAST(`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)"
_SUB_COUNT = f"SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)"
_LOGIC_SEQ = f"SAFE_CAST(`{Col.ORDER_LOGICAL_SEQ}` AS INT64)"
_PAY_AMOUNT_T2 = f"SAFE_CAST(t2.`{Col.PAYMENT_AMOUNT}` AS FLOAT64)"
```

### 広告URL IDの.0問題

BigQueryデータに `4879` と `4879.0` が混在。以下で統一:
- `bigquery_client.py`: `_norm_expr()` で `REGEXP_REPLACE(..., r'\.0$', '')`
- `common.py`: `AD_URL_NORM` 定数
- `config_loader.py`: `_normalize_ad_url_id()` で読み書き時に正規化

## ファイル構成

```
BI/
├── app.py                          # エントリーポイント（認証・ナビゲーション）
├── Dockerfile                      # Cloud Run用
├── .gcloudignore
├── requirements.txt
├── config/
│   ├── companies.yaml              # 会社マスタ
│   ├── product_cycles.yaml         # 商品サイクル
│   ├── upsell_mapping.yaml         # アップセルマッピング
│   └── ad_url_mapping.yaml         # 広告URL IDマッピング
├── pages/
│   ├── 01_cohort.py                # 分析（コホート・残存率・LTV・アップセル率）
│   ├── 02_sales.py                 # Tier分析（LTV帯別アクティブ/キャンセル）
│   ├── 03_ad_performance.py        # 広告効果（Phase2）
│   ├── 04_churn.py                 # 解約分析（キャンセル理由・回数別）
│   └── 05_master.py                # マスタ管理（商品サイクル・アップセル・広告URL ID）
├── src/
│   ├── auth.py                     # パスワード認証
│   ├── bigquery_client.py          # BQクライアント・クエリ実行・フィルタ取得
│   ├── config_loader.py            # GCS/ローカルYAML読み書き
│   ├── constants.py                # カラム名・ステータス定数
│   ├── session.py                  # セッション管理
│   ├── components/
│   │   ├── sidebar.py              # サイドバー（会社選択・日付）
│   │   ├── filters.py              # カスケードフィルタ（カテゴリ→広告G→広告URL→商品）
│   │   ├── cohort_heatmap.py       # ヒートマップ・ラインチャート
│   │   ├── metrics_row.py          # KPIカード
│   │   └── download_button.py      # Excel/CSVダウンロード
│   ├── queries/
│   │   ├── common.py               # テーブル参照・WHERE句生成・AD_URL_NORM
│   │   ├── cohort.py               # コホートSQL（残存率・LTV・アップセル）
│   │   ├── churn.py                # 解約分析SQL（キャンセル理由・回数別）
│   │   └── tier.py                 # Tier分析SQL（LTV帯別×ステータス）
│   └── transforms/
│       └── cohort_transform.py     # データ変換（残存率テーブル・分母表示等）
└── .streamlit/
    ├── config.toml                 # Streamlit UIテーマ
    └── secrets.toml                # 認証情報（gitignore対象）
```

## 各ページの機能

### 01_cohort.py（分析）
- **残存率タブ**: 月別コホート × 定期回数の残存率ヒートマップ + ラインチャート
- **LTVタブ**: 1年LTV集計テーブル + 残存分母/継続分母表示（`10/218`形式で誤認防止）
- **アップセル率タブ**: マッピングごとのアップセル率（%を`font-size:2rem`で大きく表示）
- ドリルダウン軸: 定期商品名 / 広告グループ / 商品カテゴリ

### 02_sales.py（Tier）
- LTV帯別（0~5000, 5001~10000, ..., 100001~）のアクティブ/キャンセル構成
- 積み上げ棒グラフ + KPIカード + 詳細テーブル

### 04_churn.py（解約分析）
- **タブ1 キャンセル理由**: 全体のキャンセル理由内訳（棒グラフ+テーブル）
- **タブ2 定期回数別キャンセル理由**: 顧客の最後の出荷完了回数別にキャンセル理由を表示

### 05_master.py（マスタ管理）
- **商品サイクルタブ**: 商品別cycle1/cycle2の編集（data_editor）
- **アップセルマッピングタブ**: label/分子/分母/期間デフォルトのカード形式編集
- **広告URL IDタブ**: BigQueryから全ID取得→表示名を手動入力（検索中も編集可）

## フィルタ階層

サイドバーのカスケードフィルタ:
```
商品カテゴリ → 広告グループ → 広告URL(代理店) → 定期商品名
```
- 広告URLはマスタに名前があれば「名前 (ID)」形式、なければID表示
- SQL側ではad_url_idで絞り込み（`build_filter_clause`の`ad_urls`引数）

## Streamlit再起動が必要なケース

Python 3.9環境ではモジュール変更がホットリロードされない。`src/`配下を変更した場合:

```bash
# PID確認
ps aux | grep "streamlit run app.py" | grep -v grep | awk '{print $2}'
# kill & restart
kill <PID> && sleep 2 && cd /Users/rkt_knd/Downloads/claude/BI && python -m streamlit run app.py --server.port 8501 &
```

## よくあるエラーと対処

| エラー | 原因 | 対処 |
|---|---|---|
| `No matching signature for operator = for argument types: STRING, INT64` | BigQuery全STRING型 | SAFE_CAST追加 |
| `FORMAT_DATE ... STRING` | TIMESTAMP型でないカラムにFORMAT_DATE | FORMAT_TIMESTAMP + SAFE_CAST AS TIMESTAMP |
| `Name xxx not found inside t2` | カラム名が実テーブルと不一致 | BigQueryスキーマ確認（上記参照） |
| ローカルで変更が反映されない | Streamlitがモジュールをキャッシュ | プロセスkill→再起動 |
| Cloud Runでマスタデータが消える | コンテナ再起動でファイル消失 | GCS永続化済み（config_loader.py） |
| 広告URL IDに.0が混在 | BigQueryデータの型揺れ | REGEXP_REPLACE + config_loaderで正規化済み |

## Git / GitHub

- リポジトリ: https://github.com/rikutokanda-dot/ecforce-bi-tool
- ブランチ: `main`
- ローカルパス: `/Users/rkt_knd/Downloads/claude/BI`
- ワークツリー: `/Users/rkt_knd/Downloads/claude/BI/.claude/worktrees/inspiring-mestorf`（Streamlitは本体ディレクトリで実行）
