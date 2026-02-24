from sqlalchemy.orm import Session
from models.strategy_signal import StrategySignal
from comparison_engine import ChangeEvent, ChangeType, ChangeDomain


class StrategyEngine:
    def __init__(self):
        pass

    def process(self, events: list[ChangeEvent]) -> list[StrategySignal]:

        """
        Public entry point used by run_all.py
        """
        return self.process_changes(events)

    def process_changes(self, change_events: list[ChangeEvent]) -> list[StrategySignal]:
        signals = []

        for ce in change_events:
            if ce.domain == ChangeDomain.PRICE and ce.change_type == ChangeType.INCREASE:
                signals.append(
                    self._price_increase_signal(ce)
                )

            if ce.domain == ChangeDomain.SEAT and ce.change_type == ChangeType.DECREASE:

                signals.append(
                    self._inventory_tightening_signal(ce)
                )

        return [s for s in signals if s is not None]

    def _price_increase_signal(self, ce: ChangeEvent):
        return StrategySignal(
            airline=ce.airline,
            flight_key=f"{ce.airline}-{ce.flight_number}-{ce.departure.isoformat()}",
            signal_category="PRICE_ACTION",
            signal_type="PRICE_INCREASE",
            confidence=min(1.0, abs(ce.magnitude or 0) / 0.15),
            severity=abs(ce.magnitude or 0),
            supporting_change_ids=[],  # filled AFTER persistence if needed
            context={
                "from_timestamp": ce.from_timestamp,
                "to_timestamp": ce.to_timestamp,
            }
        )

    def _inventory_tightening_signal(self, ce: ChangeEvent):
        return StrategySignal(
            airline=ce.airline,
            flight_key=f"{ce.airline}-{ce.flight_number}-{ce.departure.isoformat()}",
            signal_category="CAPACITY_ACTION",
            signal_type="INVENTORY_TIGHTENING",
            confidence=0.8,
            severity=abs(ce.magnitude or 0),
            supporting_change_ids=[],
            context={
                "from_timestamp": ce.from_timestamp,
                "to_timestamp": ce.to_timestamp,
            }
        )
