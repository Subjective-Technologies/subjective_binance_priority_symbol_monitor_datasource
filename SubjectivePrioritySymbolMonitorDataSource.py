import sys
from collections import defaultdict, deque
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from subjective_abstract_data_source_package import SubjectiveDataSource

from trading_contracts.plugin_support import icon_for, symbols_from, ticker_stream


class SubjectivePrioritySymbolMonitorDataSource(SubjectiveDataSource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.symbols = symbols_from(self._connection.get("symbols"))
        self.window = max(1, int(self._connection.get("window", 20)))
        self._windows = defaultdict(lambda: deque(maxlen=self.window))

    @classmethod
    def connection_schema(cls):
        return {
            "symbols": {"type": "textarea", "label": "Priority Symbols", "required": True},
            "window": {"type": "int", "label": "Window", "default": 20, "min": 1},
        }

    @classmethod
    def request_schema(cls):
        return {"events": {"type": "array", "label": "Market Events"}, "symbols": {"type": "array", "label": "Priority Symbols"}}

    @classmethod
    def output_schema(cls):
        return {
            "event": {"type": "object", "label": "Market Event"},
            "priority": {"type": "int", "label": "Priority"},
            "sequence": {"type": "array", "label": "Price Sequence"},
            "sma": {"type": "text", "label": "SMA"},
            "error": {"type": "text", "label": "Error"},
        }

    @classmethod
    def icon(cls):
        return icon_for(__file__)

    def supports_streaming(self):
        return True

    def stream(self, request):
        symbols = symbols_from((request or {}).get("symbols")) or self.symbols
        for event in ticker_stream(request or {}, {**self._connection, "symbols": symbols}, "multiple"):
            if event.get("event") is None and event.get("error"):
                yield {"event": None, "priority": -1, "sequence": [], "sma": "", "error": event["error"]}
                continue
            values = self._windows[event["symbol"]]
            values.append(event["last"])
            sma = format(sum(Decimal(value) for value in values) / len(values), "f") if len(values) >= self.window else ""
            yield {"event": event, "priority": symbols.index(event["symbol"]), "sequence": list(values), "sma": sma, "error": ""}

    def run(self, request):
        result = {"event": None, "priority": -1, "sequence": [], "sma": "", "error": ""}
        for result in self.stream(request or {}):
            pass
        return result
