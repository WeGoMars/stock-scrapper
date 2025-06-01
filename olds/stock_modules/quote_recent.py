import yfinance as yf
import datetime
import time
import mysql.connector
from mysql.connector import Error
from typing import Dict, List
import logging
import schedule

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockPriceFetcher:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.stock_data_buffer = []
    
    def create_table_if_not_exists(self):
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                price DECIMAL(10, 4) NULL,
                error_message TEXT NULL,
                timestamp DATETIME NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_symbol (symbol),
                INDEX idx_timestamp (timestamp)
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

    def get_us_stock_prices(self, symbols: List[str], batch_size: int = 51, delay: float = 1.0) -> Dict[str, Dict[str, str]]:
        result = {}
        self.stock_data_buffer = []

        logger.info(f"총 {len(symbols)}개 심볼 조회 시작")
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"배치 {i//batch_size + 1}: {len(batch)}개 심볼 처리 중...")

            for symbol in batch:
                timestamp = datetime.datetime.now()
                timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                try:
                    ticker = yf.Ticker(symbol)
                    current_price = ticker.fast_info.last_price

                    result[symbol] = {
                        'price': current_price,
                        'timestamp': timestamp_str
                    }

                    self.stock_data_buffer.append({
                        'symbol': symbol,
                        'price': float(current_price) if current_price is not None else None,
                        'error_message': None,
                        'timestamp': timestamp
                    })

                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"{symbol} 조회 실패: {error_msg}")
                    result[symbol] = {
                        'price': f"Error: {error_msg}",
                        'timestamp': timestamp_str
                    }
                    self.stock_data_buffer.append({
                        'symbol': symbol,
                        'price': None,
                        'error_message': error_msg,
                        'timestamp': timestamp
                    })

            if i + batch_size < len(symbols):
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

            insert_query = """
            INSERT INTO stock_prices (symbol, price, error_message, timestamp)
            VALUES (%s, %s, %s, %s)
            """

            insert_data = [
                (
                    data['symbol'],
                    data['price'],
                    data['error_message'],
                    data['timestamp']
                )
                for data in self.stock_data_buffer
            ]

            cursor.executemany(insert_query, insert_data)
            connection.commit()
            logger.info(f"DB 저장 완료: {len(insert_data)}개 레코드")
            self.stock_data_buffer = []
            return True

        except Error as e:
            logger.error(f"DB 저장 중 오류: {e}")
            if connection:
                connection.rollback()
            return False

        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def get_and_save_stock_prices(self, symbols: List[str], batch_size: int = 51, delay: float = 1.0) -> Dict[str, Dict[str, str]]:
        result = self.get_us_stock_prices(symbols, batch_size, delay)
        if self.save_to_database():
            logger.info("주식 가격 조회 및 DB 저장 완료")
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
            DELETE FROM stock_prices WHERE timestamp < %s
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

# ---------------------- 스케줄러 ----------------------
def job():
    try:
        fetcher = StockPriceFetcher(db_config)
        fetcher.create_table_if_not_exists()
        prices = fetcher.get_and_save_stock_prices(us_stocks, batch_size=51, delay=1.0)
        deleted = fetcher.delete_old_data(hours=24)
        print(f"[{datetime.datetime.now()}] 실행 완료: {len(prices)}개 심볼 수집, {deleted}개 삭제됨")
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

    us_stocks = ['A','AAPL','ABBV','ABNB','ABT','ACGL','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ','AJG','AKAM','ALB','ALGN','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','AON','AOS','APA','APD','APH','APO','APTV','ARE','ATO','AVB','AVGO','AVY','AWK','AXON','AXP','AZO','BA','BAC','BALL','BAX','BBY','BDX','BEN','BF.B','BG','BIIB','BK','BKNG','BKR','BLDR','BLK','BMY','BR','BRK.B','BRO','BSX','BX','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCL','CDNS','CDW','CEG','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL','CLX','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COO','COP','COR','COST','CPAY','CPB','CPRT','CPT','CRL','CRM','CRWD','CSCO','CSGP','CSX','CTAS','CTRA','CTSH','CTVA','CVS','CVX','CZR','D','DAL','DASH','DAY','DD','DE','DECK','DELL','DFS','DG','DGX','DHI','DHR','DIS','DLR','DLTR','DOC','DOV','DOW','DPZ','DRI','DTE','DUK','DVA','DVN','DXCM','EA','EBAY','ECL','ED','EFX','EG','EIX','EL','ELV','EMN','EMR','ENPH','EOG','EPAM','EQIX','EQR','EQT','ERIE','ES','ESS','ETN','ETR','EVRG','EW','EXC','EXE','EXPD','EXPE','EXR','F','FANG','FAST','FCX','FDS','FDX','FE','FFIV','FI','FICO','FIS','FITB','FOX','FOXA','FRT','FSLR','FTNT','FTV','GD','GDDY','GE','GEHC','GEN','GEV','GILD','GIS','GL','GLW','GM','GNRC','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS','HBAN','HCA','HD','HES','HIG','HII','HLT','HOLX','HON','HPE','HPQ','HRL','HSIC','HST','HSY','HUBB','HUM','HWM','IBM','ICE','IDXX','IEX','IFF','INCY','INTC','INTU','INVH','IP','IPG','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JBL','JCI','JKHY','JNJ','JNPR','JPM','K','KDP','KEY','KEYS','KHC','KIM','KKR','KLAC','KMB','KMI','KMX','KO','KR','KVUE','L','LDOS','LEN','LH','LHX','LII','LIN','LKQ','LLY','LMT','LNT','LOW','LRCX','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP','MCK','MCO','MDLZ','MDT','MET','META','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST','MO','MOH','MOS','MPC','MPWR','MRK','MRNA','MS','MSCI','MSFT','MSI','MTB','MTCH','MTD','MU','NCLH','NDAQ','NDSN','NEE','NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NWS','NWSA','NXPI','O','ODFL','OKE','OMC','ON','ORCL','ORLY','OTIS','OXY','PANW','PARA','PAYC','PAYX','PCAR','PCG','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PLD','PLTR','PM','PNC','PNR','PNW','PODD','POOL','PPG','PPL','PRU','PSA','PSX','PTC','PWR','PYPL','QCOM','RCL','REG','REGN','RF','RJF','RL','RMD','ROK','ROL','ROP','ROST','RSG','RTX','RVTY','SBAC','SBUX','SCHW','SHW','SJM','SLB','SMCI','SNA','SNPS','SO','SOLV','SPG','SPGI','SRE','STE','STLD','STT','STX','STZ','SW','SWK','SWKS','SYF','SYK','SYY','T','TAP','TDG','TDY','TECH','TEL','TER','TFC','TGT','TJX','TKO','TMO','TMUS','TPL','TPR','TRGP','TRMB','TROW','TRV','TSCO','TSLA','TSN','TT','TTWO','TXN','TXT','TYL','UAL','UBER','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB','V','VICI','VLO','VLTO','VMC','VRSK','VRSN','VRTX','VST','VTR','VTRS','VZ','WAB','WAT','WBA','WBD','WDAY','WDC','WEC','WELL','WFC','WM','WMB','WMT','WRB','WSM','WST','WTW','WY','WYNN','XEL','XOM','XYL','YUM','ZBH','ZBRA','ZTS']

    # 매 5분마다 job 실행
    schedule.every(5).minutes.do(job)

    print("주식 데이터 수집 스케줄러가 시작되었습니다.")
    print("종료하려면 Ctrl+C를 누르세요.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n스케줄러가 종료되었습니다.")
