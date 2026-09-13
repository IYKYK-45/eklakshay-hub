from enum import Enum
from typing import Optional
from pydantic import BaseModel,ConfigDict, Field
from datetime import datetime


class CurrencyEnum(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    INR = "INR"


class PaymentMethodEnum(str, Enum):
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    UPI = "UPI"
    NET_BANKING = "NET_BANKING"


class TransactionStatusEnum(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PENDING = "PENDING"


class TransactionPayload(BaseModel):
    transaction_id: str = Field(..., description="Unique UUID of the transaction")
    idempotency_key: str = Field(..., description="Deterministic key for deduplication")
    user_id: str = Field(..., description="Customer unique ID")
    merchant_id: str = Field(..., description="Merchant account ID")
    amount: float = Field(..., description="Transaction monetary amount")
    currency: CurrencyEnum
    payment_method: PaymentMethodEnum
    status: TransactionStatusEnum
    event_timestamp: datetime = Field(..., description="UTC ISO-8601 timestamp")

    # Metadata field injected by downstream ingestion trackers
    source_ip: Optional[str] = None

    model_config = ConfigDict(use_enum_values=True)