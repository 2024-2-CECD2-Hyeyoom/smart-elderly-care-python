import influxdb_client
import pandas as pd
from datetime import datetime, timedelta

class ActivityAnalyzer:
    def __init__(self, user_name, start_date, end_date, token, org, influx_url):
        self.user_name = user_name
        self.start_date = start_date
        self.end_date = end_date
        self.token = token
        self.org = org
        self.influx_url = influx_url
        self.client = influxdb_client.InfluxDBClient(url=influx_url, token=token, org=org)
        self.query_api = self.client.query_api()
        self.summary = None

    def analyze(self):
        """PIR 및 레이더 활동량을 날짜별로 분석"""
        start_str = self.start_date.isoformat() + "Z"
        end_str = self.end_date.isoformat() + "Z"
        
        query = f"""from(bucket: "sensor_data")
		  |> range(start: {start_str}, stop: {end_str})
		  |> filter(fn: (r) => r["_measurement"] == "PIR활동" or r["_measurement"] == "레이더활동")
		  |> filter(fn: (r) => r["_field"] == "measurement_value")
		  |> filter(fn: (r) => r["user_name"] == "{self.user_name}")
		  |> fill(column: "_value", value: 0.0)
        """
        tables = self.query_api.query(query, org=self.org)
        
        data = []
        for table in tables:
            for record in table.records:
                data.append({
                    "time": record.get_time(),
                    "measurement": record["_measurement"],
                    "value": record.get_value()
                })
                
        df = pd.DataFrame(data)
        if df.empty:
            self.summary = pd.DataFrame(columns=["날짜", "PIR 총합", "레이더 총합"])
            return
        
        # 시간 컬럼을 datetime 형식으로 변환 및 날짜 문자열 생성
        df["time"] = pd.to_datetime(df["time"])
        df["date"] = df["time"].dt.strftime('%Y-%m-%d')
        
        # 날짜별, 센서별 합산
        grouped = df.groupby(["date", "measurement"])["value"].sum().unstack(fill_value=0).reset_index()
        grouped.columns.name = None  # 멀티 인덱스 제거
        
        # 컬럼 이름 변경 및 없는 컬럼은 0으로 채움
        if "PIR활동" in grouped.columns:
            grouped = grouped.rename(columns={"PIR활동": "PIR 총합"})
        else:
            grouped["PIR 총합"] = 0
        if "레이더활동" in grouped.columns:
            grouped = grouped.rename(columns={"레이더활동": "레이더 총합"})
        else:
            grouped["레이더 총합"] = 0
        
        self.summary = grouped[["date", "PIR 총합", "레이더 총합"]].rename(columns={"date": "날짜"})

    def get_results(self):
        """날짜별 PIR 및 레이더 총 활동량 반환"""
        return self.summary
