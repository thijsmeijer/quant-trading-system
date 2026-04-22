"""Portfolio construction package."""

from quant_core.portfolio.targets import (
    DuplicateTargetWeightError,
    PortfolioTarget,
    build_portfolio_target,
)

__all__ = [
    "DuplicateTargetWeightError",
    "PortfolioTarget",
    "build_portfolio_target",
]
