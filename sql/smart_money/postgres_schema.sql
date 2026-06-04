-- Smart Money Alpha Platform canonical PostgreSQL schema.
--
-- Scope:
-- - Canonical trader/wallet registry and slowly changing state.
-- - Profile, score, ranking, cluster, regime, portfolio, risk, and backtest summaries.
-- - Latest materialized query models for service/dashboard reads.
--
-- Apply in a PostgreSQL database dedicated to rustcta platform state.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS smart_money;

CREATE TABLE IF NOT EXISTS smart_money.traders (
    trader_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name text,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_active timestamptz,
    status text NOT NULL DEFAULT 'candidate',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT traders_status_check CHECK (
        status IN ('candidate', 'active', 'probation', 'disabled', 'archived')
    ),
    CONSTRAINT traders_last_active_check CHECK (
        last_active IS NULL OR last_active >= first_seen
    )
);

CREATE TABLE IF NOT EXISTS smart_money.wallets (
    wallet_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    trader_id uuid NOT NULL REFERENCES smart_money.traders(trader_id) ON DELETE RESTRICT,
    address text NOT NULL,
    venue text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz,
    active boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT wallets_venue_check CHECK (venue IN ('hyperliquid', 'binance', 'okx', 'bybit', 'mexc', 'gate', 'htx', 'other')),
    CONSTRAINT wallets_last_seen_check CHECK (last_seen IS NULL OR last_seen >= first_seen),
    CONSTRAINT wallets_address_nonempty CHECK (length(btrim(address)) > 0),
    UNIQUE (venue, address)
);

CREATE TABLE IF NOT EXISTS smart_money.wallet_profiles (
    wallet_id uuid NOT NULL REFERENCES smart_money.wallets(wallet_id) ON DELETE CASCADE,
    as_of timestamptz NOT NULL,
    total_return numeric(38, 18) NOT NULL,
    return_30d numeric(38, 18) NOT NULL,
    return_90d numeric(38, 18) NOT NULL,
    return_180d numeric(38, 18) NOT NULL,
    sharpe numeric(38, 18),
    sortino numeric(38, 18),
    calmar numeric(38, 18),
    max_drawdown numeric(38, 18) NOT NULL,
    win_rate numeric(38, 18) NOT NULL,
    profit_factor numeric(38, 18) NOT NULL,
    average_holding_secs bigint NOT NULL,
    average_leverage numeric(38, 18) NOT NULL,
    maximum_leverage numeric(38, 18) NOT NULL,
    trade_frequency_daily numeric(38, 18) NOT NULL,
    position_concentration numeric(38, 18) NOT NULL,
    risk_per_trade numeric(38, 18) NOT NULL,
    signal_reproducibility numeric(38, 18) NOT NULL,
    capital_efficiency numeric(38, 18) NOT NULL,
    behavior_stability numeric(38, 18) NOT NULL,
    recent_performance numeric(38, 18) NOT NULL,
    closed_trades_count integer NOT NULL DEFAULT 0,
    active_days_count integer NOT NULL DEFAULT 0,
    history_days_count integer NOT NULL DEFAULT 0,
    input_window_start timestamptz,
    input_window_end timestamptz,
    inputs jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (wallet_id, as_of),
    CONSTRAINT wallet_profiles_score_bounds CHECK (
        win_rate BETWEEN 0 AND 1
        AND max_drawdown >= 0
        AND average_holding_secs >= 0
        AND average_leverage >= 0
        AND maximum_leverage >= 0
        AND trade_frequency_daily >= 0
        AND position_concentration >= 0
        AND risk_per_trade >= 0
        AND signal_reproducibility BETWEEN 0 AND 1
        AND capital_efficiency >= 0
        AND behavior_stability BETWEEN 0 AND 1
    )
);

CREATE TABLE IF NOT EXISTS smart_money.trader_cluster_history (
    trader_id uuid NOT NULL REFERENCES smart_money.traders(trader_id) ON DELETE CASCADE,
    wallet_id uuid NOT NULL REFERENCES smart_money.wallets(wallet_id) ON DELETE CASCADE,
    as_of timestamptz NOT NULL,
    cluster text NOT NULL,
    confidence numeric(38, 18) NOT NULL,
    features jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trader_id, wallet_id, as_of, cluster),
    CONSTRAINT trader_cluster_confidence_check CHECK (confidence BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS smart_money.wallet_score_history (
    wallet_id uuid NOT NULL REFERENCES smart_money.wallets(wallet_id) ON DELETE CASCADE,
    as_of timestamptz NOT NULL,
    trend_score numeric(38, 18) NOT NULL,
    range_score numeric(38, 18) NOT NULL,
    high_volatility_score numeric(38, 18) NOT NULL,
    low_volatility_score numeric(38, 18) NOT NULL,
    risk_on_score numeric(38, 18) NOT NULL,
    risk_off_score numeric(38, 18) NOT NULL,
    current_regime_score numeric(38, 18) NOT NULL,
    recent_performance_score numeric(38, 18) NOT NULL,
    consistency_score numeric(38, 18) NOT NULL,
    signal_quality_score numeric(38, 18) NOT NULL,
    execution_quality_score numeric(38, 18) NOT NULL,
    risk_adjusted_return_score numeric(38, 18) NOT NULL DEFAULT 0,
    final_score numeric(38, 18) NOT NULL,
    eligible boolean NOT NULL DEFAULT true,
    ineligibility_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    inputs jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (wallet_id, as_of)
);

CREATE TABLE IF NOT EXISTS smart_money.wallet_ranking_history (
    wallet_id uuid NOT NULL REFERENCES smart_money.wallets(wallet_id) ON DELETE CASCADE,
    as_of timestamptz NOT NULL,
    universe text NOT NULL,
    rank integer NOT NULL,
    percentile numeric(38, 18) NOT NULL,
    score numeric(38, 18) NOT NULL,
    eligible boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (wallet_id, as_of, universe),
    UNIQUE (as_of, universe, rank),
    CONSTRAINT wallet_ranking_rank_check CHECK (rank > 0),
    CONSTRAINT wallet_ranking_percentile_check CHECK (percentile BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS smart_money.market_regime_history (
    regime_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    as_of timestamptz NOT NULL UNIQUE,
    universe text NOT NULL DEFAULT 'binance_usdt_perp',
    bull_trend numeric(38, 18) NOT NULL,
    bear_trend numeric(38, 18) NOT NULL,
    range_score numeric(38, 18) NOT NULL,
    volatility_expansion numeric(38, 18) NOT NULL,
    volatility_compression numeric(38, 18) NOT NULL,
    risk_on numeric(38, 18) NOT NULL,
    risk_off numeric(38, 18) NOT NULL,
    funding_extreme numeric(38, 18) NOT NULL,
    liquidity_crisis numeric(38, 18) NOT NULL,
    trend_acceleration numeric(38, 18) NOT NULL,
    trend_exhaustion numeric(38, 18) NOT NULL,
    dominant_regime text,
    inputs jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS smart_money.target_portfolios (
    portfolio_id uuid NOT NULL DEFAULT gen_random_uuid(),
    as_of timestamptz NOT NULL,
    version integer NOT NULL DEFAULT 1,
    nav_usdt numeric(38, 8) NOT NULL,
    gross_notional_usdt numeric(38, 8) NOT NULL,
    max_gross_notional_usdt numeric(38, 8) NOT NULL DEFAULT 20000,
    leverage numeric(38, 18) NOT NULL,
    max_leverage numeric(38, 18) NOT NULL DEFAULT 10,
    standard_entry_notional_usdt numeric(38, 8) NOT NULL DEFAULT 1000,
    target_weights jsonb NOT NULL,
    target_notional jsonb NOT NULL,
    cash_weight numeric(38, 18) NOT NULL,
    source_alpha_snapshot timestamptz NOT NULL,
    source_regime_as_of timestamptz,
    status text NOT NULL DEFAULT 'proposed',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (portfolio_id, as_of),
    CONSTRAINT target_portfolios_status_check CHECK (
        status IN ('proposed', 'risk_approved', 'risk_scaled', 'risk_rejected', 'executed', 'superseded')
    ),
    CONSTRAINT target_portfolios_nav_check CHECK (nav_usdt > 0),
    CONSTRAINT target_portfolios_hard_limits_check CHECK (
        gross_notional_usdt >= 0
        AND max_gross_notional_usdt > 0
        AND gross_notional_usdt <= max_gross_notional_usdt
        AND leverage >= 0
        AND max_leverage > 0
        AND leverage <= max_leverage
        AND standard_entry_notional_usdt > 0
    )
);

CREATE TABLE IF NOT EXISTS smart_money.risk_decisions (
    decision_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id uuid NOT NULL,
    target_as_of timestamptz NOT NULL,
    decided_at timestamptz NOT NULL DEFAULT now(),
    decision text NOT NULL,
    scaled_weights jsonb,
    scaled_notional jsonb,
    reject_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    risk_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    policy_version text NOT NULL DEFAULT 'v1',
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (portfolio_id, target_as_of)
        REFERENCES smart_money.target_portfolios(portfolio_id, as_of)
        ON DELETE CASCADE,
    CONSTRAINT risk_decisions_decision_check CHECK (decision IN ('approved', 'scaled', 'rejected', 'close_only')),
    CONSTRAINT risk_decisions_reject_reasons_array_check CHECK (jsonb_typeof(reject_reasons) = 'array')
);

CREATE TABLE IF NOT EXISTS smart_money.backtest_runs (
    run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_name text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running',
    dataset_start timestamptz NOT NULL,
    dataset_end timestamptz NOT NULL,
    initial_nav_usdt numeric(38, 8) NOT NULL DEFAULT 2000,
    max_gross_notional_usdt numeric(38, 8) NOT NULL DEFAULT 20000,
    max_leverage numeric(38, 18) NOT NULL DEFAULT 10,
    taker_fee_rate numeric(38, 18) NOT NULL DEFAULT 0.0004,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT backtest_runs_status_check CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    CONSTRAINT backtest_runs_window_check CHECK (dataset_end > dataset_start)
);

CREATE TABLE IF NOT EXISTS smart_money.backtest_results (
    run_id uuid PRIMARY KEY REFERENCES smart_money.backtest_runs(run_id) ON DELETE CASCADE,
    completed_at timestamptz NOT NULL DEFAULT now(),
    final_nav_usdt numeric(38, 8) NOT NULL,
    total_return numeric(38, 18) NOT NULL,
    annualized_return numeric(38, 18),
    sharpe numeric(38, 18),
    sortino numeric(38, 18),
    calmar numeric(38, 18),
    max_drawdown numeric(38, 18) NOT NULL,
    gross_pnl_usdt numeric(38, 8) NOT NULL,
    fees_usdt numeric(38, 8) NOT NULL,
    funding_usdt numeric(38, 8) NOT NULL,
    slippage_usdt numeric(38, 8) NOT NULL,
    fills_count bigint NOT NULL,
    partial_fills_count bigint NOT NULL,
    portfolio_count bigint NOT NULL,
    risk_reject_count bigint NOT NULL,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    artifacts jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT backtest_results_counts_check CHECK (
        fills_count >= 0
        AND partial_fills_count >= 0
        AND portfolio_count >= 0
        AND risk_reject_count >= 0
    )
);

CREATE INDEX IF NOT EXISTS traders_status_last_active_idx
    ON smart_money.traders (status, last_active DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS wallets_trader_active_idx
    ON smart_money.wallets (trader_id, active, last_seen DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS wallets_venue_active_idx
    ON smart_money.wallets (venue, active, last_seen DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS wallet_profiles_as_of_idx
    ON smart_money.wallet_profiles (as_of DESC);

CREATE INDEX IF NOT EXISTS wallet_profiles_quality_idx
    ON smart_money.wallet_profiles (
        profit_factor DESC,
        max_drawdown ASC,
        maximum_leverage ASC,
        active_days_count DESC
    );

CREATE INDEX IF NOT EXISTS trader_cluster_history_cluster_as_of_idx
    ON smart_money.trader_cluster_history (cluster, as_of DESC);

CREATE INDEX IF NOT EXISTS wallet_score_history_as_of_final_score_idx
    ON smart_money.wallet_score_history (as_of DESC, final_score DESC)
    WHERE eligible;

CREATE INDEX IF NOT EXISTS wallet_ranking_history_universe_as_of_rank_idx
    ON smart_money.wallet_ranking_history (universe, as_of DESC, rank ASC);

CREATE INDEX IF NOT EXISTS market_regime_history_universe_as_of_idx
    ON smart_money.market_regime_history (universe, as_of DESC);

CREATE INDEX IF NOT EXISTS target_portfolios_as_of_status_idx
    ON smart_money.target_portfolios (as_of DESC, status);

CREATE INDEX IF NOT EXISTS risk_decisions_target_idx
    ON smart_money.risk_decisions (portfolio_id, target_as_of, decided_at DESC);

CREATE INDEX IF NOT EXISTS backtest_runs_status_started_idx
    ON smart_money.backtest_runs (status, started_at DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS smart_money.latest_wallet_profiles AS
SELECT DISTINCT ON (wallet_id)
    *
FROM smart_money.wallet_profiles
ORDER BY wallet_id, as_of DESC;

CREATE UNIQUE INDEX IF NOT EXISTS latest_wallet_profiles_wallet_id_idx
    ON smart_money.latest_wallet_profiles (wallet_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS smart_money.latest_wallet_scores AS
SELECT DISTINCT ON (wallet_id)
    *
FROM smart_money.wallet_score_history
ORDER BY wallet_id, as_of DESC;

CREATE UNIQUE INDEX IF NOT EXISTS latest_wallet_scores_wallet_id_idx
    ON smart_money.latest_wallet_scores (wallet_id);

CREATE INDEX IF NOT EXISTS latest_wallet_scores_final_score_idx
    ON smart_money.latest_wallet_scores (eligible, final_score DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS smart_money.latest_wallet_rankings AS
SELECT DISTINCT ON (wallet_id, universe)
    *
FROM smart_money.wallet_ranking_history
ORDER BY wallet_id, universe, as_of DESC;

CREATE UNIQUE INDEX IF NOT EXISTS latest_wallet_rankings_wallet_universe_idx
    ON smart_money.latest_wallet_rankings (wallet_id, universe);

CREATE INDEX IF NOT EXISTS latest_wallet_rankings_universe_rank_idx
    ON smart_money.latest_wallet_rankings (universe, rank ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS smart_money.latest_trader_clusters AS
SELECT DISTINCT ON (trader_id, wallet_id, cluster)
    *
FROM smart_money.trader_cluster_history
ORDER BY trader_id, wallet_id, cluster, as_of DESC;

CREATE UNIQUE INDEX IF NOT EXISTS latest_trader_clusters_key_idx
    ON smart_money.latest_trader_clusters (trader_id, wallet_id, cluster);

CREATE MATERIALIZED VIEW IF NOT EXISTS smart_money.latest_market_regime AS
SELECT DISTINCT ON (universe)
    *
FROM smart_money.market_regime_history
ORDER BY universe, as_of DESC;

CREATE UNIQUE INDEX IF NOT EXISTS latest_market_regime_universe_idx
    ON smart_money.latest_market_regime (universe);

CREATE MATERIALIZED VIEW IF NOT EXISTS smart_money.latest_target_portfolios AS
SELECT DISTINCT ON (portfolio_id)
    *
FROM smart_money.target_portfolios
ORDER BY portfolio_id, as_of DESC;

CREATE UNIQUE INDEX IF NOT EXISTS latest_target_portfolios_portfolio_id_idx
    ON smart_money.latest_target_portfolios (portfolio_id);

CREATE INDEX IF NOT EXISTS latest_target_portfolios_status_as_of_idx
    ON smart_money.latest_target_portfolios (status, as_of DESC);
