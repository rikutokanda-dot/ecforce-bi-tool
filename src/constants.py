"""BigQueryカラム名・ステータス値の定数定義."""

PROJECT_ID = "ecforce-data"
BQ_LOCATION = "asia-northeast1"
MAX_RETENTION_MONTHS = 12


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
    AD_GROUP = "受注_広告url_グループ名"
    PRODUCT_CATEGORY = "定期受注_受注商品_商品カテゴリ"
    PAYMENT_AMOUNT = "受注_決済金額"


class Status:
    """ステータス値."""

    SHIPPED = "shipped"
    COMPLETED = "completed"


class LogicalSeq:
    """論理連番の値."""

    FIRST = 1
    REPROCESS = 2
    # NULL = 失敗データ


# ドリルダウン軸の定義
DRILLDOWN_OPTIONS = {
    "なし": None,
    "定期商品名": Col.PRODUCT_NAME,
    "広告グループ": Col.AD_GROUP,
    "商品カテゴリ": Col.PRODUCT_CATEGORY,
}
