"""
=============================================================================
  Capital.com REST API Client
  ─────────────────────────────────────────────────────────────────────────
  Official API docs: https://open-api.capital.com/
  Base URL (live):   https://api-capital.backend-capital.com/
  Base URL (demo):   https://demo-api-capital.backend-capital.com/

  Authentication flow:
    1. POST /api/v1/session  →  receive CST + X-SECURITY-TOKEN headers
    2. Include both headers in every subsequent request
    3. Session expires after 10 min of inactivity — auto-renewed by this client
=============================================================================
"""

import time
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


class CapitalComAPI:
    """
    Thread-safe Capital.com REST API client with automatic session renewal.

    Usage:
        api = CapitalComAPI(api_key="...", identifier="email@example.com",
                            password="your_api_key_password", demo=True)
        api.create_session()
        account = api.get_account_info()
        api.open_position("GOLD", direction="BUY", size=1,
                          stop_level=1800.0, profit_level=1900.0)
    """

    LIVE_URL = "https://api-capital.backend-capital.com"
    DEMO_URL = "https://demo-api-capital.backend-capital.com"

    def __init__(self, api_key: str, identifier: str, password: str, demo: bool = True):
        """
        Args:
            api_key:    Your Capital.com API key (from Settings > API integrations)
            identifier: Your Capital.com login email
            password:   The custom password you set when generating the API key
                        (NOT your account login password)
            demo:       True = use demo account, False = use live account
        """
        self.api_key = api_key
        self.identifier = identifier
        self.password = password
        self.demo = demo
        self.base_url = self.DEMO_URL if demo else self.LIVE_URL

        self._cst = None
        self._security_token = None
        self._session_created_at = None
        self._session = requests.Session()
        self._session.headers.update({
            "X-CAP-API-KEY": self.api_key,
            "Content-Type": "application/json",
        })

    # ─────────────────────────────────────────────────────────────────────
    # SESSION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────

    def create_session(self, retries: int = 3) -> bool:
        """
        Start a new API session. Must be called before any trading operations.
        Retries up to `retries` times on connection errors.
        Returns True on success.
        """
        url = f"{self.base_url}/api/v1/session"
        payload = {
            "identifier": self.identifier,
            "password": self.password,
            "encryptedPassword": False,
        }
        for attempt in range(1, retries + 1):
            try:
                # Recreate the underlying requests session to clear stale connections
                self._session = requests.Session()
                self._session.headers.update({
                    "X-CAP-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                })
                resp = self._session.post(url, json=payload, timeout=15)
                if resp.status_code == 200:
                    self._cst = resp.headers.get("CST")
                    self._security_token = resp.headers.get("X-SECURITY-TOKEN")
                    self._session_created_at = time.time()
                    self._session.headers.update({
                        "CST": self._cst,
                        "X-SECURITY-TOKEN": self._security_token,
                    })
                    data = resp.json()
                    logger.info(f"Session created | Account: {data.get('accountType')} | "
                               f"Currency: {data.get('currencyIsoCode')} | "
                               f"Mode: {'DEMO' if self.demo else 'LIVE'}")
                    return True
                else:
                    logger.error(f"Session creation failed (attempt {attempt}/{retries}): "
                                 f"{resp.status_code} — {resp.text}")
            except Exception as e:
                logger.error(f"Session creation error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                wait = attempt * 5
                logger.info(f"Retrying session in {wait}s...")
                time.sleep(wait)
        return False

    def _ensure_session(self):
        """Auto-renew session if it's been more than 9 minutes since last creation."""
        if self._session_created_at is None:
            raise RuntimeError("No active session. Call create_session() first.")
        elapsed = time.time() - self._session_created_at
        if elapsed > 540:  # 9 minutes
            logger.info("Session expiring soon — renewing...")
            success = self.create_session(retries=3)
            if not success:
                raise RuntimeError("Session renewal failed after 3 attempts.")

    def delete_session(self):
        """Close the current API session."""
        self._ensure_session()
        self._session.delete(f"{self.base_url}/api/v1/session", timeout=10)
        logger.info("Session closed.")

    # ─────────────────────────────────────────────────────────────────────
    # ACCOUNT
    # ─────────────────────────────────────────────────────────────────────

    def get_account_info(self) -> dict:
        """
        Returns account details including balance, equity, and currency.
        """
        self._ensure_session()
        resp = self._session.get(f"{self.base_url}/api/v1/accounts", timeout=10)
        resp.raise_for_status()
        accounts = resp.json().get("accounts", [])
        # Return the preferred (active) account
        for acc in accounts:
            if acc.get("preferred"):
                return acc
        return accounts[0] if accounts else {}

    def get_account_preferences(self) -> dict:
        """Get leverage and hedging mode settings."""
        self._ensure_session()
        resp = self._session.get(f"{self.base_url}/api/v1/accounts/preferences", timeout=10)
        resp.raise_for_status()
        return resp.json()

    # ─────────────────────────────────────────────────────────────────────
    # MARKET DATA
    # ─────────────────────────────────────────────────────────────────────

    def search_markets(self, search_term: str) -> list:
        """
        Search for tradeable instruments by name or ticker.
        Returns a list of market dicts with 'epic', 'instrumentName', etc.

        Example:
            markets = api.search_markets("Gold")
            epic = markets[0]['epic']  # e.g., "GOLD"
        """
        self._ensure_session()
        resp = self._session.get(
            f"{self.base_url}/api/v1/markets",
            params={"searchTerm": search_term, "limit": 10},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json().get("markets", [])

    def get_market_details(self, epic: str) -> dict:
        """Get full details for a specific instrument (epic)."""
        self._ensure_session()
        resp = self._session.get(f"{self.base_url}/api/v1/markets/{epic}", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_prices(self, epic: str, resolution: str = "HOUR",
                   max_bars: int = 100) -> list:
        """
        Fetch historical OHLCV price bars.

        Args:
            epic:       Instrument epic (e.g., "GOLD", "EURUSD", "NATURALGAS")
            resolution: MINUTE, MINUTE_5, MINUTE_15, MINUTE_30, HOUR,
                        HOUR_4, DAY, WEEK
            max_bars:   Number of bars to return (max 1000)

        Returns:
            List of dicts with openPrice, highPrice, lowPrice, closePrice, lastTradedVolume
        """
        self._ensure_session()
        resp = self._session.get(
            f"{self.base_url}/api/v1/prices/{epic}",
            params={"resolution": resolution, "max": max_bars},
            timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("prices", [])

    def get_current_price(self, epic: str) -> dict:
        """Get the current bid/ask price for an instrument."""
        self._ensure_session()
        resp = self._session.get(f"{self.base_url}/api/v1/markets/{epic}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        snapshot = data.get("snapshot", {})
        return {
            "epic": epic,
            "bid": snapshot.get("bid"),
            "offer": snapshot.get("offer"),
            "spread": (snapshot.get("offer", 0) or 0) - (snapshot.get("bid", 0) or 0),
            "status": snapshot.get("marketStatus"),
        }

    def is_market_open(self, epic: str) -> bool:
        """
        Check if a market is currently open and tradeable.
        Capital.com returns marketStatus = 'TRADEABLE' when open.
        Returns True only when the market can accept new orders.
        """
        try:
            price = self.get_current_price(epic)
            status = price.get("status", "")
            return status == "TRADEABLE"
        except Exception as e:
            logger.warning(f"Could not check market status for {epic}: {e}")
            return False

    # ─────────────────────────────────────────────────────────────────────
    # POSITIONS (Open Trades)
    # ─────────────────────────────────────────────────────────────────────

    def get_all_positions(self) -> list:
        """Get all currently open positions."""
        self._ensure_session()
        resp = self._session.get(f"{self.base_url}/api/v1/positions", timeout=10)
        resp.raise_for_status()
        return resp.json().get("positions", [])

    def get_positions(self) -> list:
        """
        Alias for get_all_positions() — added for v3.0 bot.py compatibility.
        Both names return the same list of open positions.
        """
        return self.get_all_positions()

    def get_open_positions(self) -> list:
        """
        Alias for get_all_positions() — added for v3.0 bot.py compatibility.
        Both names return the same list of open positions.
        """
        return self.get_all_positions()

    def open_position(self, epic: str, direction: str, size: float,
                      stop_level: Optional[float] = None,
                      profit_level: Optional[float] = None,
                      stop_distance: Optional[float] = None,
                      profit_distance: Optional[float] = None,
                      guaranteed_stop: bool = False) -> dict:
        """
        Open a new CFD position on Capital.com.

        Args:
            epic:             Instrument epic (e.g., "GOLD", "EURUSD", "NATURALGAS")
            direction:        "BUY" (long) or "SELL" (short)
            size:             Position size in lots/units
            stop_level:       Absolute stop-loss price level
            profit_level:     Absolute take-profit price level
            stop_distance:    Stop-loss distance from current price (alternative to stop_level)
            profit_distance:  Take-profit distance from current price
            guaranteed_stop:  Use guaranteed stop (may incur extra cost)

        Returns:
            dict with dealReference (use to confirm the trade)
        """
        self._ensure_session()
        payload = {
            "epic": epic,
            "direction": direction.upper(),
            "size": size,
            "guaranteedStop": guaranteed_stop,
        }
        if stop_level is not None:
            payload["stopLevel"] = stop_level
        if profit_level is not None:
            payload["profitLevel"] = profit_level
        if stop_distance is not None:
            payload["stopDistance"] = stop_distance
        if profit_distance is not None:
            payload["profitDistance"] = profit_distance

        resp = self._session.post(
            f"{self.base_url}/api/v1/positions",
            json=payload,
            timeout=15
        )
        if resp.status_code in (200, 201):
            result = resp.json()
            deal_ref = result.get("dealReference")
            logger.info(f"Position opened: {direction} {size} {epic} | dealRef={deal_ref}")
            return result
        else:
            logger.error(f"Open position failed: {resp.status_code} — {resp.text}")
            resp.raise_for_status()

    def close_position(self, deal_id: str) -> dict:
        """
        Close an open position by its dealId.

        Args:
            deal_id: The dealId of the position to close (from get_all_positions())
        """
        self._ensure_session()
        resp = self._session.delete(
            f"{self.base_url}/api/v1/positions/{deal_id}",
            timeout=15
        )
        if resp.status_code in (200, 201):
            logger.info(f"Position closed: dealId={deal_id}")
            return resp.json()
        else:
            logger.error(f"Close position failed: {resp.status_code} — {resp.text}")
            resp.raise_for_status()

    def close_position_partial(self, deal_id: str, direction: str, size: float) -> dict:
        """
        Partially close an open position by placing an opposing order of the given size.

        Capital.com does not have a dedicated partial-close endpoint.
        The standard approach is to open an opposing position with the partial size,
        which the broker nets off against the existing position.

        Args:
            deal_id:   The dealId of the open position (used for logging only)
            direction: Opposing direction — 'SELL' to partially close a BUY, 'BUY' for SELL
            size:      Number of units to close (must be < full position size)

        Returns:
            dict with dealReference
        """
        self._ensure_session()

        # Fetch the epic from the position first
        positions = self.get_all_positions()
        epic = None
        for p in positions:
            if p.get('position', {}).get('dealId') == deal_id:
                epic = p.get('market', {}).get('epic')
                break

        if not epic:
            raise ValueError(f'close_position_partial: could not find epic for dealId={deal_id}')

        payload = {
            'epic':           epic,
            'direction':      direction.upper(),
            'size':           size,
            'guaranteedStop': False,
        }
        resp = self._session.post(
            f'{self.base_url}/api/v1/positions',
            json=payload,
            timeout=15
        )
        if resp.status_code in (200, 201):
            result = resp.json()
            logger.info(
                f'Partial close: {direction} {size} {epic} | '
                f'dealId={deal_id} | dealRef={result.get("dealReference")}'
            )
            return result
        else:
            logger.error(f'Partial close failed: {resp.status_code} — {resp.text}')
            resp.raise_for_status()

    def update_position(self, deal_id: str,
                        stop_level: Optional[float] = None,
                        profit_level: Optional[float] = None) -> dict:
        """Update stop-loss or take-profit on an existing position."""
        self._ensure_session()
        payload = {}
        if stop_level is not None:
            payload["stopLevel"] = stop_level
        if profit_level is not None:
            payload["profitLevel"] = profit_level

        resp = self._session.put(
            f"{self.base_url}/api/v1/positions/{deal_id}",
            json=payload,
            timeout=15
        )
        resp.raise_for_status()
        return resp.json()

    def confirm_deal(self, deal_reference: str) -> dict:
        """
        Confirm whether a deal was successfully executed.
        Always call this after open_position() to verify the trade went through.
        """
        self._ensure_session()
        resp = self._session.get(
            f"{self.base_url}/api/v1/confirms/{deal_reference}",
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()

    # ─────────────────────────────────────────────────────────────────────
    # WORKING ORDERS (Limit / Stop Orders)
    # ─────────────────────────────────────────────────────────────────────

    def place_limit_order(self, epic: str, direction: str, size: float,
                          level: float, stop_level: Optional[float] = None,
                          profit_level: Optional[float] = None) -> dict:
        """
        Place a limit or stop entry order (executes when price reaches 'level').

        Args:
            epic:         Instrument epic
            direction:    "BUY" or "SELL"
            size:         Position size
            level:        Target entry price
            stop_level:   Stop-loss price
            profit_level: Take-profit price
        """
        self._ensure_session()
        payload = {
            "epic": epic,
            "direction": direction.upper(),
            "size": size,
            "level": level,
            "type": "LIMIT",
        }
        if stop_level:
            payload["stopLevel"] = stop_level
        if profit_level:
            payload["profitLevel"] = profit_level

        resp = self._session.post(
            f"{self.base_url}/api/v1/workingorders",
            json=payload,
            timeout=15
        )
        resp.raise_for_status()
        return resp.json()

    def get_working_orders(self) -> list:
        """Get all pending working orders."""
        self._ensure_session()
        resp = self._session.get(f"{self.base_url}/api/v1/workingorders", timeout=10)
        resp.raise_for_status()
        return resp.json().get("workingOrders", [])

    def cancel_working_order(self, deal_id: str) -> dict:
        """Cancel a pending working order."""
        self._ensure_session()
        resp = self._session.delete(
            f"{self.base_url}/api/v1/workingorders/{deal_id}",
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()

    # ─────────────────────────────────────────────────────────────────────
    # TRADE HISTORY
    # ─────────────────────────────────────────────────────────────────────

    def get_trade_history(self, from_date: Optional[str] = None,
                          to_date: Optional[str] = None,
                          last_period: int = 86400) -> list:
        """
        Get closed trade history.

        Args:
            from_date:   ISO 8601 string e.g. "2026-01-01T00:00:00"
            to_date:     ISO 8601 string
            last_period: Seconds to look back (default 86400 = last 24h)
        """
        self._ensure_session()
        params = {"lastPeriod": last_period}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        resp = self._session.get(
            f"{self.base_url}/api/v1/history/transactions",
            params=params,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json().get("items", [])

    def get_closed_positions(self, from_date: Optional[str] = None,
                             to_date: Optional[str] = None,
                             last_period: int = 86400) -> list:
        """
        Alias for get_trade_history() — added for v3.0 bot.py compatibility.
        Returns closed trade history.
        """
        return self.get_trade_history(from_date=from_date, to_date=to_date,
                                      last_period=last_period)

    # ─────────────────────────────────────────────────────────────────────
    # CONVENIENCE HELPERS (v3.0 compatibility)
    # ─────────────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        """
        Return the current account balance as a float.
        Convenience method for v3.0 bot.py compatibility.
        """
        acc = self.get_account_info()
        return float(acc.get('balance', {}).get('balance', 0))

    def get_account_balance(self) -> dict:
        """
        Return the full balance dict (balance, deposit, profitLoss, available).
        Convenience method for v3.0 bot.py compatibility.
        """
        acc = self.get_account_info()
        return acc.get('balance', {})
