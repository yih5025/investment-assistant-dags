from sqlalchemy import Column, Integer, String, DateTime, Float
from app.database import Base
from app.models.base import BaseModel

class BithumbTicker(BaseModel):
    __tablename__ = "bithumb_ticker"
    
    id = Column(Integer, primary_key=True, index=True)
    market = Column(String, index=True)
    trade_date = Column(String)
    trade_time = Column(String)
    trade_date_kst = Column(String)
    trade_time_kst = Column(String)
    trade_timestamp = Column(Integer)
    opening_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    trade_price = Column(Float)
    prev_closing_price = Column(Float)
    change = Column(String)
    change_price = Column(Float)
    change_rate = Column(Float)
    signed_change_price = Column(Float)
    signed_change_rate = Column(Float)
    trade_volume = Column(Float)
    acc_trade_price = Column(Float)
    acc_trade_price_24h = Column(Float)
    acc_trade_volume = Column(Float)
    acc_trade_volume_24h = Column(Float)
    highest_52_week_price = Column(Float)
    highest_52_week_date = Column(String)
    lowest_52_week_price = Column(Float)
    lowest_52_week_date = Column(String)
    timestamp_field = Column(Integer)
    source = Column(String)