# 종목테이블

## stock_prices

CREATE TABLE IF NOT EXISTS stock_prices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(10),
        datetime DATETIME,
        open DECIMAL(10,5),
        high DECIMAL(10,5),
        low DECIMAL(10,5),
        close DECIMAL(10,5),
        volume BIGINT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT NULL,
        UNIQUE KEY unique_symbol_datetime (symbol, datetime)

## sp500_returns
 CREATE TABLE IF NOT EXISTS sp500_returns (
        id INT AUTO_INCREMENT PRIMARY KEY,
        period VARCHAR(10),
        base_date DATE,
        `current_date` DATE,
        base_price DECIMAL(10,2),
        current_price DECIMAL(10,2),
        return_percent DECIMAL(6,2),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_period_date (period, `current_date`)
    )


## interest_rates
 CREATE TABLE IF NOT EXISTS interest_rates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            series_id VARCHAR(50),
            date DATE,
            value FLOAT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT NULL,
            UNIQUE KEY unique_series_date (series_id, date)
        )

## sectors_performance
 CREATE TABLE IF NOT EXISTS sectors_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sector VARCHAR(255),
            change_percentage FLOAT,
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_sector_fetched (sector, fetched_at)
        )


## fred_vix
 CREATE TABLE IF NOT EXISTS fred_vix (
        id INT AUTO_INCREMENT PRIMARY KEY,
        observation_date DATE NOT NULL,
        value DECIMAL(10,4),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT NULL,
        UNIQUE KEY unique_observation_date (observation_date)
    )


## stock_financials
 CREATE TABLE IF NOT EXISTS stock_financials (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(10),
        industry VARCHAR(255),
        sector VARCHAR(255),
        market_cap BIGINT,
        roe FLOAT,
        eps FLOAT,
        bps FLOAT,
        beta FLOAT,
        dividend_yield FLOAT,
        current_ratio FLOAT,
        debt_ratio FLOAT,
        target_date DATE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT NULL,
        UNIQUE KEY unique_symbol_date (symbol, target_date)
    )
