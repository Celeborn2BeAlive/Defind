from __future__ import annotations
from typing import NewType, Literal

Address = NewType("Address", str)   # 0x-prefixed, lowercase
Topic0  = NewType("Topic0", str)    # 66-char 0x-hash
Status  = Literal["pending", "done", "failed"]
EventKind = Literal["Mint", "Burn", "Collect", "CollectFees"]