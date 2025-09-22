import asyncio
import json
import time
import logging
import websockets

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class FinnhubETFWebSocketTest:
    """Finnhub WebSocket으로 ETF 데이터 수집 테스트"""
    
    def __init__(self):
        # 테스트할 ETF 심볼들 (인기 ETF 10개)
        self.etf_symbols = [
            'SPY',   # SPDR S&P 500 ETF
            'QQQ',   # Invesco QQQ ETF  
            'VTI',   # Vanguard Total Stock Market ETF
        ]
        
        self.api_key = 'd06vv49r01qg26saj2t0d06vv49r01qg26saj2tg'
        self.websocket = None
        self.message_count = 0
        self.etf_data_received = {}  # ETF별 수신된 데이터 카운트
        
    async def connect_websocket(self):
        """WebSocket 연결"""
        try:
            uri = f"wss://ws.finnhub.io?token={self.api_key}"
            self.websocket = await websockets.connect(uri)
            logger.info("✅ Finnhub WebSocket 연결 성공")
            
            # ETF 심볼들 구독
            for symbol in self.etf_symbols:
                subscribe_msg = {"type": "subscribe", "symbol": symbol}
                await self.websocket.send(json.dumps(subscribe_msg))
                await asyncio.sleep(0.1)  # 구독 간 딜레이
                logger.info(f"📊 ETF 구독: {symbol}")
                
            logger.info(f"✅ {len(self.etf_symbols)}개 ETF 심볼 구독 완료")
            
        except Exception as e:
            logger.error(f"❌ WebSocket 연결 실패: {e}")
            raise
    
    async def process_message(self, message):
        """메시지 처리 및 ETF 데이터 분석"""
        try:
            data = json.loads(message)
            
            # 거래 데이터만 처리
            if data.get('type') == 'trade' and data.get('data'):
                for trade in data['data']:
                    symbol = trade.get('s')
                    price = trade.get('p')
                    volume = trade.get('v')
                    timestamp = trade.get('t')
                    
                    # ETF 심볼인지 확인
                    if symbol in self.etf_symbols:
                        # ETF 데이터 카운트 증가
                        self.etf_data_received[symbol] = self.etf_data_received.get(symbol, 0) + 1
                        
                        print(f"🎯 ETF 데이터: {symbol} | 가격: ${price} | 거래량: {volume} | 시간: {timestamp}")
                        
                        # 각 ETF별 통계 출력
                        if self.etf_data_received[symbol] % 5 == 0:  # 5개마다 요약
                            print(f"📊 {symbol} 누적: {self.etf_data_received[symbol]}개 거래 데이터")
                    
            self.message_count += 1
            
            # 100개 메시지마다 전체 통계 출력
            if self.message_count % 100 == 0:
                self.print_statistics()
                
        except json.JSONDecodeError:
            logger.warning(f"⚠️ JSON 파싱 실패: {message}")
        except Exception as e:
            logger.error(f"❌ 메시지 처리 실패: {e}")
    
    def print_statistics(self):
        """ETF 데이터 수집 통계 출력"""
        print("\n" + "="*60)
        print(f"📈 ETF 데이터 수집 통계 (총 메시지: {self.message_count}개)")
        print("="*60)
        
        total_etf_trades = sum(self.etf_data_received.values())
        print(f"🎯 총 ETF 거래 데이터: {total_etf_trades}개")
        
        # ETF별 상세 통계
        for symbol in self.etf_symbols:
            count = self.etf_data_received.get(symbol, 0)
            status = "✅ 활성" if count > 0 else "⭕ 대기중"
            print(f"   {symbol}: {count:3d}개 거래 - {status}")
        
        # 성공률 계산
        active_etfs = len([s for s in self.etf_symbols if self.etf_data_received.get(s, 0) > 0])
        success_rate = (active_etfs / len(self.etf_symbols)) * 100
        print(f"📊 활성 ETF: {active_etfs}/{len(self.etf_symbols)} ({success_rate:.1f}%)")
        
        print("="*60 + "\n")
    
    async def run_test(self, duration_seconds=60):
        """ETF 데이터 수집 테스트 실행"""
        logger.info(f"🚀 Finnhub ETF WebSocket 테스트 시작 ({duration_seconds}초)")
        
        try:
            await self.connect_websocket()
            
            start_time = time.time()
            
            # 지정된 시간동안 데이터 수집
            while time.time() - start_time < duration_seconds:
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=1)
                    await self.process_message(message)
                    
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("⚠️ 연결 끊김")
                    break
            
            # 최종 결과 출력
            self.print_final_results()
            
        except Exception as e:
            logger.error(f"❌ 테스트 실행 중 오류: {e}")
        finally:
            if self.websocket:
                await self.websocket.close()
                logger.info("🔌 WebSocket 연결 종료")
    
    def print_final_results(self):
        """최종 테스트 결과 출력"""
        print("\n" + "🎉 ETF 데이터 수집 테스트 완료!")
        print("="*70)
        
        total_trades = sum(self.etf_data_received.values())
        active_etfs = len([s for s in self.etf_symbols if self.etf_data_received.get(s, 0) > 0])
        
        print(f"📊 최종 결과:")
        print(f"   총 메시지: {self.message_count}개")
        print(f"   ETF 거래 데이터: {total_trades}개")
        print(f"   활성 ETF: {active_etfs}/{len(self.etf_symbols)}개")
        
        if total_trades > 0:
            print(f"\n✅ 결론: Finnhub WebSocket에서 ETF 데이터 수집 가능!")
            print(f"   평균 ETF당 거래 데이터: {total_trades/len(self.etf_symbols):.1f}개")
        else:
            print(f"\n❌ 결론: 테스트 시간 동안 ETF 데이터 없음")
            print(f"   더 긴 시간으로 재테스트 권장")
        
        print("="*70)

# 테스트 실행 함수
async def main():
    """메인 테스트 함수"""
    tester = FinnhubETFWebSocketTest()
    
    print("🎯 Finnhub WebSocket ETF 데이터 수집 가능성 테스트")
    print(f"📋 테스트 대상 ETF: {', '.join(tester.etf_symbols)}")
    print("⏰ 60초 동안 데이터 수집 테스트를 시작합니다...\n")
    
    await tester.run_test(duration_seconds=60)

if __name__ == "__main__":
    # 테스트 실행
    asyncio.run(main())