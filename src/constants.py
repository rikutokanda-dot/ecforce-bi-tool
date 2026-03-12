"""BigQueryカラム名・ステータス値の定数定義."""

PROJECT_ID = "ecforce-data"
BQ_LOCATION = "asia-northeast1"
MAX_RETENTION_MONTHS = 24
LTV_PERIOD_DAYS = 365
PROCESSING_BUFFER_DAYS = 10  # 定期受注作成日から1回目出荷・解決までの猶予日数


class Col:
    """BigQueryテーブルのカラム名."""

    CUSTOMER_ID = "顧客_id"
    ORDER_ID = "受注_id"
    SUBSCRIPTION_CREATED_AT = "定期受注_作成日時"
    ORDER_SUBSCRIPTION_COUNT = "受注_定期回数"
    ORDER_LOGICAL_SEQ = "受注_論理連番"
    ORDER_STATUS = "受注_対応状況"
    PAYMENT_STATUS = "受注_決済状況"
    PRODUCT_NAME = "受注_受注商品_商品名"
    SUBSCRIPTION_PRODUCT_NAME = "定期受注_受注商品_商品名"
    AD_GROUP = "受注_広告url_グループ名"
    PRODUCT_CATEGORY = "定期受注_受注商品_商品カテゴリ"
    PAYMENT_AMOUNT = "受注_決済金額"
    CANCEL_REASON = "定期受注_キャンセル理由名"
    CANCEL_DATE = "定期受注_キャンセル日時"
    AD_URL_PARAM = "初回受注_広告url_パラメータ"
    SUBSCRIPTION_STATUS = "定期受注_ステータス"
    SALES_DATE = "受注_売上日時"
    ORDER_CREATED_DATE = "受注_作成日時_yyyymmdd"


class Status:
    """ステータス値."""

    SHIPPED = "shipped"
    COMPLETED = "completed"

    # 返品率分析用: 総出荷（分母）に含む対応状況
    TOTAL_SHIPPED_STATUSES = (
        "shipped",              # 発送完了
        "returned",             # 返品
        "backproduct",          # 配送戻り
        "returnedpaid",         # 返品【返金済】
        "block",                # 受取拒否
        "waitingforaccount",    # 口座まち
        "accountrevealed",      # 口座判明・返品まち
        "cancel_notarrived",    # キャンセル予定・返品まち
        "shipped_notarrived",   # 発送完了（未着）
    )

    # 1回目分母から除外するステータス
    COHORT_EXCLUDED_STATUSES = (
        "complete",                    # 注文確定
        "reserved",                    # 予約販売 / 予約
        "additional",                  # 追加請求
        "additionalbilling",           # 追加請求（clinic）
        "calling",                     # 架電中
        "waiting_exam",                # 問診待ち
        "waiting_contact",             # 連絡待ち
        "発送待ち",                     # 発送待ち
        "発送準備",                     # 発送準備
        "出荷準備",                     # 出荷準備
        "cc_breakaway",                # キャンセル（CC_FT）
        "cc_phoneisnotconnected",      # キャンセル（CC_RD）
        "doc_breakaway",               # キャンセル（Doc_FT）
        "doc_phoneisnotconnected",     # キャンセル（Doc_RD）
        "キャンセル_既存定期再開",       # キャンセル_既存定期再開
    )

    # 返品率分析用: 返品（分子）に含む対応状況
    RETURN_STATUSES = (
        "returned",             # 返品
        "backproduct",          # 配送戻り
        "returnedpaid",         # 返品【返金済】
        "block",                # 受取拒否
        "waitingforaccount",    # 口座まち（返品受付中）
        "accountrevealed",      # 口座判明・返品まち（返品受付中）
        "cancel_notarrived",    # キャンセル予定・返品まち（返品受付中）
    )


class LogicalSeq:
    """論理連番の値."""

    FIRST = 1
    REPROCESS = 2
    # NULL = 失敗データ


# ドリルダウン軸の定義 (先頭がデフォルト)
DRILLDOWN_OPTIONS = {
    "定期商品名": Col.SUBSCRIPTION_PRODUCT_NAME,
    "広告グループ": Col.AD_GROUP,
    "商品カテゴリ": Col.PRODUCT_CATEGORY,
    "広告URLパラメータ": Col.AD_URL_PARAM,
}
