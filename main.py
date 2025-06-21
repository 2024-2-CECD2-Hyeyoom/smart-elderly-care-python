from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from datetime import datetime

from datetime import datetime
from sleep_analyzer import SleepAnalyzer
from outing_analyzer import OutingAnalyzer

app = FastAPI()

class SensorDataDTO(BaseModel):
    sensor_type_name: str
    measurement_time: str
    measurement_values: List[float]


class SleepEventDTO(BaseModel):
    sleepStartTime: str
    sleepEndTime: str
    sleepDurationMinutes: int

class OutingEventDTO(BaseModel):
    outingStartTime: str
    outingEndTime: str
    outingDurationMinutes: int

class AnalysisResult(BaseModel):
    sleepEvents: List[SleepEventDTO]
    outingEvents: List[OutingEventDTO]

def convert_to_sleep_events(df_sleep_periods):
    sleep_events = []
    for _, row in df_sleep_periods.iterrows():
        start_time = row['sleep_start'].to_pydatetime()
        end_time = row['wake_time'].to_pydatetime()
        duration_minutes = int((end_time - start_time).total_seconds() // 60)
        sleep_events.append({
            "sleepStartTime": start_time,
            "sleepEndTime": end_time,
            "sleepDurationMinutes": duration_minutes
        })
    return sleep_events


def convert_to_outing_events(df_outing_periods):
    outing_events = []
    for _, row in df_outing_periods.iterrows():
        start_time = row['outing_start'].to_pydatetime()
        end_time = row['outing_end'].to_pydatetime()
        duration_minutes = int((end_time - start_time).total_seconds() // 60)
        outing_events.append({
            "outingStartTime": start_time,
            "outingEndTime": end_time,
            "outingDurationMinutes": duration_minutes
        })
    return outing_events

@app.post("/analyze-sensor", response_model=AnalysisResult)
async def analyze_sensor(data: List[SensorDataDTO]):
    print(f"수신된 센서: {len(data)}개")

    sensor_json_data = [
        {
            "sensor": d.sensor_type_name,
            "time": d.measurement_time,
            "values": d.measurement_values
        }
        for d in data
    ]
    print(sensor_json_data)

    times = [datetime.fromisoformat(d.measurement_time.replace("Z", "")) for d in data]
    start_date = min(times).date()
    end_date = max(times).date()

    sleep_analyzer = SleepAnalyzer(
        user_name="UserA",
        sensor_json_data=sensor_json_data,
        start_date=start_date,
        end_date=end_date
    )
    sleep_analyzer.analyze()
    _, df_sleep_periods, _ = sleep_analyzer.get_results()

    sleep_events = []
    for _, row in df_sleep_periods.iterrows():
        sleep_events.append(SleepEventDTO(
            sleepStartTime=row['sleep_start'].to_pydatetime().isoformat(),
            sleepEndTime=row['wake_time'].to_pydatetime().isoformat(),
            sleepDurationMinutes=int((row['wake_time'] - row['sleep_start']).total_seconds() // 60)
        ))

    outing_analyzer = OutingAnalyzer(
        sensor_json_data=sensor_json_data,
    )
    outing_analyzer.analyze()
    df_outing_periods = outing_analyzer.get_results()

    outing_events = []
    for _, row in df_outing_periods.iterrows():
        start_time = datetime.fromisoformat(row['outing_start'])
        end_time = datetime.fromisoformat(row['outing_end'])

        outing_events.append(OutingEventDTO(
            outingStartTime=start_time.isoformat(),
            outingEndTime=end_time.isoformat(),
            outingDurationMinutes=int((end_time - start_time).total_seconds() // 60)
        ))

    # 수면 이벤트 출력
    print("[수면 이벤트]")
    for event in sleep_events:
        print(f"- 시작: {event.sleepStartTime}, 종료: {event.sleepEndTime}, 총 {event.sleepDurationMinutes}분")

    # 외출 이벤트 출력
    print("[외출 이벤트]")
    for event in outing_events:
        print(f"- 시작: {event.outingStartTime}, 종료: {event.outingEndTime}, 총 {event.outingDurationMinutes}분")

    return AnalysisResult(sleepEvents=sleep_events, outingEvents=outing_events)
