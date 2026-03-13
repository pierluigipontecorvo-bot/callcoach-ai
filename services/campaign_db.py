"""
Campaign configuration lookup with longest-prefix matching.

How the matching works
──────────────────────
Given a full campaign code such as  INTER-MG.-2954-JAMILLE-(VI)
the lookup generates candidates from most to least specific:

  1. INTER-MG.-2954-JAMILLE-(VI)   ← exact match
  2. INTER-MG.-2954-JAMILLE
  3. INTER-MG.-2954
  4. INTER-MG.
  5. INTER                         ← type-level default

The first candidate that has an active row in the `campaigns` table wins.

This means:
- A single "INTER" row covers all INTER campaigns by default.
- Adding a more specific row (e.g. "INTER-MG.-2954") creates an exception
  that overrides the default only for that sub-set — no code changes needed.
"""

import logging
from typing import Optional

from sqlalchemy import select

from database import AsyncSessionLocal
from models import Campaign

logger = logging.getLogger(__name__)


async def get_campaign_by_code(campaign_code: str) -> Optional[Campaign]:
    """
    Return the most specific active Campaign config for *campaign_code*,
    using longest-prefix matching (see module docstring).
    Returns None if no match is found.
    """
    # Build candidate list from most to least specific
    tokens = campaign_code.split("-")
    candidates = ["-".join(tokens[:i]) for i in range(len(tokens), 0, -1)]

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Campaign)
            .where(Campaign.code.in_(candidates))
            .where(Campaign.active.is_(True))
        )
        matches: dict[str, Campaign] = {c.code: c for c in result.scalars().all()}

    if not matches:
        logger.info("No campaign config found for code=%r (tried %d candidates)", campaign_code, len(candidates))
        return None

    # Return the first candidate that was found (= most specific)
    for candidate in candidates:
        if candidate in matches:
            logger.info(
                "Campaign config matched: code=%r → pattern=%r (nome=%r)",
                campaign_code, candidate, matches[candidate].nome,
            )
            return matches[candidate]

    return None  # unreachable, but satisfies type checkers
