import yfinance as yf
import datetime
import time
import mysql.connector
from mysql.connector import Error
from typing import Dict, List, Optional, Any
import logging
import schedule
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockDataFetcher:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.stock_data_buffer = []
    
    def create_table_if_not_exists(self, recreate: bool = False):
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            # 테이블 재생성이 필요한 경우
            if recreate:
                drop_query = "DROP TABLE IF EXISTS stock_comprehensive_data"
                cursor.execute(drop_query)
                logger.info("기존 테이블 삭제됨")
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_comprehensive_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                company_name VARCHAR(255) NULL,
                sector VARCHAR(100) NULL,
                industry VARCHAR(200) NULL,
                country VARCHAR(100) NULL,
                exchange VARCHAR(50) NULL,
                currency VARCHAR(10) NULL,
                website VARCHAR(500) NULL,
                
                -- 가격 정보
                current_price DECIMAL(15, 4) NULL,
                previous_close DECIMAL(15, 4) NULL,
                open_price DECIMAL(15, 4) NULL,
                day_low DECIMAL(15, 4) NULL,
                day_high DECIMAL(15, 4) NULL,
                fifty_two_week_low DECIMAL(15, 4) NULL,
                fifty_two_week_high DECIMAL(15, 4) NULL,
                fifty_day_average DECIMAL(15, 4) NULL,
                two_hundred_day_average DECIMAL(15, 4) NULL,
                
                -- 시장 지표
                market_cap BIGINT NULL,
                enterprise_value BIGINT NULL,
                shares_outstanding BIGINT NULL,
                float_shares BIGINT NULL,
                volume BIGINT NULL,
                average_volume BIGINT NULL,
                average_volume_10days BIGINT NULL,
                
                -- 재무 비율
                trailing_pe DECIMAL(10, 4) NULL,
                forward_pe DECIMAL(10, 4) NULL,
                price_to_book DECIMAL(10, 4) NULL,
                price_to_sales DECIMAL(10, 4) NULL,
                enterprise_to_revenue DECIMAL(10, 4) NULL,
                enterprise_to_ebitda DECIMAL(10, 4) NULL,
                peg_ratio DECIMAL(10, 4) NULL,
                beta DECIMAL(10, 4) NULL,
                
                -- 수익성 지표
                profit_margins DECIMAL(10, 6) NULL,
                gross_margins DECIMAL(10, 6) NULL,
                operating_margins DECIMAL(10, 6) NULL,
                ebitda_margins DECIMAL(10, 6) NULL,
                return_on_assets DECIMAL(10, 6) NULL,
                return_on_equity DECIMAL(10, 6) NULL,
                
                -- 재무 건전성
                total_cash BIGINT NULL,
                total_cash_per_share DECIMAL(10, 4) NULL,
                total_debt BIGINT NULL,
                debt_to_equity DECIMAL(10, 4) NULL,
                current_ratio DECIMAL(10, 4) NULL,
                quick_ratio DECIMAL(10, 4) NULL,
                
                -- 배당 정보
                dividend_rate DECIMAL(10, 4) NULL,
                dividend_yield DECIMAL(10, 6) NULL,
                ex_dividend_date DATE NULL,
                payout_ratio DECIMAL(10, 6) NULL,
                five_year_avg_dividend_yield DECIMAL(10, 6) NULL,
                trailing_annual_dividend_rate DECIMAL(10, 4) NULL,
                trailing_annual_dividend_yield DECIMAL(10, 6) NULL,
                
                -- 성장률
                earnings_growth DECIMAL(10, 6) NULL,
                revenue_growth DECIMAL(10, 6) NULL,
                earnings_quarterly_growth DECIMAL(10, 6) NULL,
                revenue_quarterly_growth DECIMAL(10, 6) NULL,
                
                -- 애널리스트 정보
                recommendation_key VARCHAR(20) NULL,
                recommendation_mean DECIMAL(4, 2) NULL,
                number_of_analyst_opinions INT NULL,
                target_high_price DECIMAL(15, 4) NULL,
                target_low_price DECIMAL(15, 4) NULL,
                target_mean_price DECIMAL(15, 4) NULL,
                
                -- EPS 정보
                trailing_eps DECIMAL(10, 4) NULL,
                forward_eps DECIMAL(10, 4) NULL,
                book_value DECIMAL(10, 4) NULL,
                
                -- 기타 정보
                last_split_date DATE NULL,
                last_split_factor VARCHAR(20) NULL,
                most_recent_quarter DATE NULL,
                next_fiscal_year_end DATE NULL,
                
                error_message TEXT NULL,
                timestamp DATETIME NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                INDEX idx_symbol (symbol),
                INDEX idx_timestamp (timestamp),
                INDEX idx_sector (sector),
                INDEX idx_industry (industry)
            )
            """
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("테이블 생성/확인 완료")
        except Error as e:
            logger.error(f"테이블 생성 중 오류: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def safe_get_value(self, data: Dict, key: str, default=None) -> Any:
        """안전하게 딕셔너리에서 값을 가져오는 함수"""
        try:
            value = data.get(key, default)
            if value in [None, 'N/A', '', 'None']:
                return None
            return value
        except:
            return None

    def safe_convert_to_date(self, timestamp) -> Optional[str]:
        """타임스탬프를 날짜로 안전하게 변환"""
        try:
            if timestamp is None:
                return None
            if isinstance(timestamp, (int, float)):
                return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            return None
        except:
            return None

    def get_comprehensive_stock_data(self, symbols: List[str], batch_size: int = 30, delay: float = 2.0) -> Dict[str, Dict[str, Any]]:
        result = {}
        self.stock_data_buffer = []

        logger.info(f"총 {len(symbols)}개 심볼 조회 시작 (상세 정보 포함)")
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"배치 {i//batch_size + 1}: {len(batch)}개 심볼 처리 중...")

            for symbol in batch:
                timestamp = datetime.datetime.now()
                timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                try:
                    ticker = yf.Ticker(symbol)
                    
                    # 기본 정보 및 가격 정보 가져오기
                    info = ticker.info
                    
                    # 데이터 구조 생성
                    stock_data = {
                        'symbol': symbol,
                        'timestamp': timestamp,
                        'error_message': None,
                        
                        # 기본 회사 정보
                        'company_name': self.safe_get_value(info, 'longName'),
                        'sector': self.safe_get_value(info, 'sector'),
                        'industry': self.safe_get_value(info, 'industry'),
                        'country': self.safe_get_value(info, 'country'),
                        'exchange': self.safe_get_value(info, 'exchange'),
                        'currency': self.safe_get_value(info, 'currency'),
                        'website': self.safe_get_value(info, 'website'),
                        
                        # 가격 정보
                        'current_price': self.safe_get_value(info, 'currentPrice'),
                        'previous_close': self.safe_get_value(info, 'previousClose'),
                        'open_price': self.safe_get_value(info, 'open'),
                        'day_low': self.safe_get_value(info, 'dayLow'),
                        'day_high': self.safe_get_value(info, 'dayHigh'),
                        'fifty_two_week_low': self.safe_get_value(info, 'fiftyTwoWeekLow'),
                        'fifty_two_week_high': self.safe_get_value(info, 'fiftyTwoWeekHigh'),
                        'fifty_day_average': self.safe_get_value(info, 'fiftyDayAverage'),
                        'two_hundred_day_average': self.safe_get_value(info, 'twoHundredDayAverage'),
                        
                        # 시장 지표
                        'market_cap': self.safe_get_value(info, 'marketCap'),
                        'enterprise_value': self.safe_get_value(info, 'enterpriseValue'),
                        'shares_outstanding': self.safe_get_value(info, 'sharesOutstanding'),
                        'float_shares': self.safe_get_value(info, 'floatShares'),
                        'volume': self.safe_get_value(info, 'volume'),
                        'average_volume': self.safe_get_value(info, 'averageVolume'),
                        'average_volume_10days': self.safe_get_value(info, 'averageVolume10days'),
                        
                        # 재무 비율
                        'trailing_pe': self.safe_get_value(info, 'trailingPE'),
                        'forward_pe': self.safe_get_value(info, 'forwardPE'),
                        'price_to_book': self.safe_get_value(info, 'priceToBook'),
                        'price_to_sales': self.safe_get_value(info, 'priceToSalesTrailing12Months'),
                        'enterprise_to_revenue': self.safe_get_value(info, 'enterpriseToRevenue'),
                        'enterprise_to_ebitda': self.safe_get_value(info, 'enterpriseToEbitda'),
                        'peg_ratio': self.safe_get_value(info, 'pegRatio'),
                        'beta': self.safe_get_value(info, 'beta'),
                        
                        # 수익성 지표
                        'profit_margins': self.safe_get_value(info, 'profitMargins'),
                        'gross_margins': self.safe_get_value(info, 'grossMargins'),
                        'operating_margins': self.safe_get_value(info, 'operatingMargins'),
                        'ebitda_margins': self.safe_get_value(info, 'ebitdaMargins'),
                        'return_on_assets': self.safe_get_value(info, 'returnOnAssets'),
                        'return_on_equity': self.safe_get_value(info, 'returnOnEquity'),
                        
                        # 재무 건전성
                        'total_cash': self.safe_get_value(info, 'totalCash'),
                        'total_cash_per_share': self.safe_get_value(info, 'totalCashPerShare'),
                        'total_debt': self.safe_get_value(info, 'totalDebt'),
                        'debt_to_equity': self.safe_get_value(info, 'debtToEquity'),
                        'current_ratio': self.safe_get_value(info, 'currentRatio'),
                        'quick_ratio': self.safe_get_value(info, 'quickRatio'),
                        
                        # 배당 정보
                        'dividend_rate': self.safe_get_value(info, 'dividendRate'),
                        'dividend_yield': self.safe_get_value(info, 'dividendYield'),
                        'ex_dividend_date': self.safe_convert_to_date(self.safe_get_value(info, 'exDividendDate')),
                        'payout_ratio': self.safe_get_value(info, 'payoutRatio'),
                        'five_year_avg_dividend_yield': self.safe_get_value(info, 'fiveYearAvgDividendYield'),
                        'trailing_annual_dividend_rate': self.safe_get_value(info, 'trailingAnnualDividendRate'),
                        'trailing_annual_dividend_yield': self.safe_get_value(info, 'trailingAnnualDividendYield'),
                        
                        # 성장률
                        'earnings_growth': self.safe_get_value(info, 'earningsGrowth'),
                        'revenue_growth': self.safe_get_value(info, 'revenueGrowth'),
                        'earnings_quarterly_growth': self.safe_get_value(info, 'earningsQuarterlyGrowth'),
                        'revenue_quarterly_growth': self.safe_get_value(info, 'revenueQuarterlyGrowth'),
                        
                        # 애널리스트 정보
                        'recommendation_key': self.safe_get_value(info, 'recommendationKey'),
                        'recommendation_mean': self.safe_get_value(info, 'recommendationMean'),
                        'number_of_analyst_opinions': self.safe_get_value(info, 'numberOfAnalystOpinions'),
                        'target_high_price': self.safe_get_value(info, 'targetHighPrice'),
                        'target_low_price': self.safe_get_value(info, 'targetLowPrice'),
                        'target_mean_price': self.safe_get_value(info, 'targetMeanPrice'),
                        
                        # EPS 정보
                        'trailing_eps': self.safe_get_value(info, 'trailingEps'),
                        'forward_eps': self.safe_get_value(info, 'forwardEps'),
                        'book_value': self.safe_get_value(info, 'bookValue'),
                        
                        # 기타 정보
                        'last_split_date': self.safe_convert_to_date(self.safe_get_value(info, 'lastSplitDate')),
                        'last_split_factor': self.safe_get_value(info, 'lastSplitFactor'),
                        'most_recent_quarter': self.safe_convert_to_date(self.safe_get_value(info, 'mostRecentQuarter')),
                        'next_fiscal_year_end': self.safe_convert_to_date(self.safe_get_value(info, 'nextFiscalYearEnd')),
                    }

                    result[symbol] = {
                        'status': 'success',
                        'data': stock_data,
                        'timestamp': timestamp_str
                    }

                    self.stock_data_buffer.append(stock_data)
                    logger.info(f"{symbol} 데이터 수집 완료")

                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"{symbol} 조회 실패: {error_msg}")
                    
                    error_data = {
                        'symbol': symbol,
                        'timestamp': timestamp,
                        'error_message': error_msg,
                        # 모든 필드를 None으로 설정
                        **{field: None for field in [
                            'company_name', 'sector', 'industry', 'country', 'exchange', 'currency', 'website',
                            'current_price', 'previous_close', 'open_price', 'day_low', 'day_high',
                            'fifty_two_week_low', 'fifty_two_week_high', 'fifty_day_average', 'two_hundred_day_average',
                            'market_cap', 'enterprise_value', 'shares_outstanding', 'float_shares', 'volume',
                            'average_volume', 'average_volume_10days', 'trailing_pe', 'forward_pe', 'price_to_book',
                            'price_to_sales', 'enterprise_to_revenue', 'enterprise_to_ebitda', 'peg_ratio', 'beta',
                            'profit_margins', 'gross_margins', 'operating_margins', 'ebitda_margins',
                            'return_on_assets', 'return_on_equity', 'total_cash', 'total_cash_per_share',
                            'total_debt', 'debt_to_equity', 'current_ratio', 'quick_ratio', 'dividend_rate',
                            'dividend_yield', 'ex_dividend_date', 'payout_ratio', 'five_year_avg_dividend_yield',
                            'trailing_annual_dividend_rate', 'trailing_annual_dividend_yield', 'earnings_growth',
                            'revenue_growth', 'earnings_quarterly_growth', 'revenue_quarterly_growth',
                            'recommendation_key', 'recommendation_mean', 'number_of_analyst_opinions',
                            'target_high_price', 'target_low_price', 'target_mean_price', 'trailing_eps',
                            'forward_eps', 'book_value', 'last_split_date', 'last_split_factor',
                            'most_recent_quarter', 'next_fiscal_year_end'
                        ]}
                    }
                    
                    result[symbol] = {
                        'status': 'error',
                        'error': error_msg,
                        'timestamp': timestamp_str
                    }
                    
                    self.stock_data_buffer.append(error_data)

            if i + batch_size < len(symbols):
                logger.info(f"다음 배치까지 {delay}초 대기...")
                time.sleep(delay)

        logger.info(f"데이터 조회 완료. {len(self.stock_data_buffer)}개 레코드가 메모리에 저장됨")
        return result

    def save_to_database(self) -> bool:
        if not self.stock_data_buffer:
            logger.warning("저장할 데이터가 없습니다.")
            return False

        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            # 필드 리스트 정의
            fields = [
                'symbol', 'company_name', 'sector', 'industry', 'country', 'exchange', 'currency', 'website',
                'current_price', 'previous_close', 'open_price', 'day_low', 'day_high',
                'fifty_two_week_low', 'fifty_two_week_high', 'fifty_day_average', 'two_hundred_day_average',
                'market_cap', 'enterprise_value', 'shares_outstanding', 'float_shares', 'volume',
                'average_volume', 'average_volume_10days', 'trailing_pe', 'forward_pe', 'price_to_book',
                'price_to_sales', 'enterprise_to_revenue', 'enterprise_to_ebitda', 'peg_ratio', 'beta',
                'profit_margins', 'gross_margins', 'operating_margins', 'ebitda_margins',
                'return_on_assets', 'return_on_equity', 'total_cash', 'total_cash_per_share',
                'total_debt', 'debt_to_equity', 'current_ratio', 'quick_ratio', 'dividend_rate',
                'dividend_yield', 'ex_dividend_date', 'payout_ratio', 'five_year_avg_dividend_yield',
                'trailing_annual_dividend_rate', 'trailing_annual_dividend_yield', 'earnings_growth',
                'revenue_growth', 'earnings_quarterly_growth', 'revenue_quarterly_growth',
                'recommendation_key', 'recommendation_mean', 'number_of_analyst_opinions',
                'target_high_price', 'target_low_price', 'target_mean_price', 'trailing_eps',
                'forward_eps', 'book_value', 'last_split_date', 'last_split_factor',
                'most_recent_quarter', 'next_fiscal_year_end', 'error_message', 'timestamp'
            ]

            # INSERT 쿼리 생성
            placeholders = ', '.join(['%s'] * len(fields))
            insert_query = f"""
            INSERT INTO stock_comprehensive_data ({', '.join(fields)})
            VALUES ({placeholders})
            """

            # 데이터 준비
            insert_data = []
            for data in self.stock_data_buffer:
                row = []
                for field in fields:
                    value = data.get(field)
                    row.append(value)
                insert_data.append(tuple(row))

            logger.info(f"필드 개수: {len(fields)}, 첫 번째 데이터 행의 값 개수: {len(insert_data[0]) if insert_data else 0}")
            
            cursor.executemany(insert_query, insert_data)
            connection.commit()
            logger.info(f"DB 저장 완료: {len(insert_data)}개 레코드")
            self.stock_data_buffer = []
            return True

        except Error as e:
            logger.error(f"DB 저장 중 오류: {e}")
            logger.error(f"쿼리: {insert_query[:200]}...")
            if insert_data:
                logger.error(f"첫 번째 데이터 샘플: {insert_data[0]}")
            if connection:
                connection.rollback()
            return False

        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def get_and_save_stock_data(self, symbols: List[str], batch_size: int = 30, delay: float = 2.0) -> Dict[str, Dict[str, Any]]:
        result = self.get_comprehensive_stock_data(symbols, batch_size, delay)
        if self.save_to_database():
            logger.info("주식 상세 데이터 조회 및 DB 저장 완료")
        else:
            logger.error("DB 저장 실패")
        return result

    def delete_old_data(self, hours: int = 24) -> int:
        """지정된 시간보다 오래된 데이터 삭제"""
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            threshold_time = datetime.datetime.now() - datetime.timedelta(hours=hours)
            delete_query = """
            DELETE FROM stock_comprehensive_data WHERE timestamp < %s
            """
            cursor.execute(delete_query, (threshold_time,))
            deleted_rows = cursor.rowcount
            connection.commit()
            logger.info(f"{deleted_rows}개 레코드 삭제됨 (기준: {hours}시간)")
            return deleted_rows
        except Error as e:
            logger.error(f"오래된 데이터 삭제 중 오류: {e}")
            return 0
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def get_buffer_data(self) -> List[Dict]:
        return self.stock_data_buffer.copy()

    def get_data_summary(self) -> Dict[str, int]:
        """수집된 데이터의 요약 정보 반환"""
        if not self.stock_data_buffer:
            return {}
        
        success_count = len([data for data in self.stock_data_buffer if data['error_message'] is None])
        error_count = len([data for data in self.stock_data_buffer if data['error_message'] is not None])
        
        sectors = {}
        for data in self.stock_data_buffer:
            if data['sector'] and data['error_message'] is None:
                sectors[data['sector']] = sectors.get(data['sector'], 0) + 1
        
        return {
            'total_symbols': len(self.stock_data_buffer),
            'success_count': success_count,
            'error_count': error_count,
            'sectors': sectors
        }

# ---------------------- 스케줄러 ----------------------
def job():
    try:
        fetcher = StockDataFetcher(db_config)
        fetcher.create_table_if_not_exists()
        
        # 배치 사이즈를 줄이고 지연시간을 늘려서 API 제한 회피
        result = fetcher.get_and_save_stock_data(us_stocks, batch_size=20, delay=3.0)
        deleted = fetcher.delete_old_data(hours=24)
        
        # 요약 정보 출력
        summary = fetcher.get_data_summary()
        print(f"[{datetime.datetime.now()}] 실행 완료:")
        print(f"  - 총 심볼: {summary.get('total_symbols', 0)}개")
        print(f"  - 성공: {summary.get('success_count', 0)}개")
        print(f"  - 실패: {summary.get('error_count', 0)}개")
        print(f"  - 삭제된 레코드: {deleted}개")
        
        if summary.get('sectors'):
            print("  - 섹터별 분포:")
            for sector, count in list(summary['sectors'].items())[:5]:  # 상위 5개 섹터만 표시
                print(f"    {sector}: {count}개")
                
    except Exception as e:
        logger.error(f"스케줄링 작업 중 오류: {e}")

if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'database': 'test',
        'user': 'root',
        'password': 'admin',
        'port': 3306
    }

    # S&P 500 주식 심볼 (샘플을 위해 처음 50개만 사용)
    us_stocks = ['A','AAPL','ABBV','ABNB','ABT','ACGL','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ','AJG','AKAM','ALB','ALGN','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','AON','AOS','APA','APD','APH','APO','APTV','ARE','ATO','AVB','AVGO','AVY','AWK','AXON','AXP','AZO','BA','BAC','BALL','BAX','BBY','BDX','BEN','BF.B','BG','BIIB','BK','BKNG','BKR','BLDR','BLK','BMY','BR','BRK.B','BRO','BSX','BX','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCL','CDNS','CDW','CEG','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL','CLX','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COO','COP','COR','COST','CPAY','CPB','CPRT','CPT','CRL','CRM','CRWD','CSCO','CSGP','CSX','CTAS','CTRA','CTSH','CTVA','CVS','CVX','CZR','D','DAL','DASH','DAY','DD','DE','DECK','DELL','DFS','DG','DGX','DHI','DHR','DIS','DLR','DLTR','DOC','DOV','DOW','DPZ','DRI','DTE','DUK','DVA','DVN','DXCM','EA','EBAY','ECL','ED','EFX','EG','EIX','EL','ELV','EMN','EMR','ENPH','EOG','EPAM','EQIX','EQR','EQT','ERIE','ES','ESS','ETN','ETR','EVRG','EW','EXC','EXE','EXPD','EXPE','EXR','F','FANG','FAST','FCX','FDS','FDX','FE','FFIV','FI','FICO','FIS','FITB','FOX','FOXA','FRT','FSLR','FTNT','FTV','GD','GDDY','GE','GEHC','GEN','GEV','GILD','GIS','GL','GLW','GM','GNRC','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS','HBAN','HCA','HD','HES','HIG','HII','HLT','HOLX','HON','HPE','HPQ','HRL','HSIC','HST','HSY','HUBB','HUM','HWM','IBM','ICE','IDXX','IEX','IFF','INCY','INTC','INTU','INVH','IP','IPG','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JBL','JCI','JKHY','JNJ','JNPR','JPM','K','KDP','KEY','KEYS','KHC','KIM','KKR','KLAC','KMB','KMI','KMX','KO','KR','KVUE','L','LDOS','LEN','LH','LHX','LII','LIN','LKQ','LLY','LMT','LNT','LOW','LRCX','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP','MCK','MCO','MDLZ','MDT','MET','META','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST','MO','MOH','MOS','MPC','MPWR','MRK','MRNA','MS','MSCI','MSFT','MSI','MTB','MTCH','MTD','MU','NCLH','NDAQ','NDSN','NEE','NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NWS','NWSA','NXPI','O','ODFL','OKE','OMC','ON','ORCL','ORLY','OTIS','OXY','PANW','PARA','PAYC','PAYX','PCAR','PCG','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PLD','PLTR','PM','PNC','PNR','PNW','PODD','POOL','PPG','PPL','PRU','PSA','PSX','PTC','PWR','PYPL','QCOM','RCL','REG','REGN','RF','RJF','RL','RMD','ROK','ROL','ROP','ROST','RSG','RTX','RVTY','SBAC','SBUX','SCHW','SHW','SJM','SLB','SMCI','SNA','SNPS','SO','SOLV','SPG','SPGI','SRE','STE','STLD','STT','STX','STZ','SW','SWK','SWKS','SYF','SYK','SYY','T','TAP','TDG','TDY','TECH','TEL','TER','TFC','TGT','TJX','TKO','TMO','TMUS','TPL','TPR','TRGP','TRMB','TROW','TRV','TSCO','TSLA','TSN','TT','TTWO','TXN','TXT','TYL','UAL','UBER','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB','V','VICI','VLO','VLTO','VMC','VRSK','VRSN','VRTX','VST','VTR','VTRS','VZ','WAB','WAT','WBA','WBD','WDAY','WDC','WEC','WELL','WFC','WM','WMB','WMT','WRB','WSM','WST','WTW','WY','WYNN','XEL','XOM','XYL','YUM','ZBH','ZBRA','ZTS']

    # 매 30분마다 job 실행 (API 제한을 고려하여 간격 증가)
    schedule.every(30).minutes.do(job)

    print("주식 상세 데이터 수집 스케줄러가 시작되었습니다.")
    print("더 많은 정보를 수집하므로 처리 시간이 길어질 수 있습니다.")
    print("종료하려면 Ctrl+C를 누르세요.")

    try:
        # 처음 한 번 즉시 실행
        print("초기 데이터 수집을 시작합니다...")
        job()
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n스케줄러가 종료되었습니다.")