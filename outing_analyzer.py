from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)

class OutingAnalyzer:
    def __init__(self, sensor_json_data,
                 threshold_heart_breath=10, threshold_radar_pir=15,
                 threshold_home_activity=50, alpha=0.005):
        self.raw_data = sensor_json_data
        self.threshold_heart_breath = threshold_heart_breath
        self.threshold_radar_pir = threshold_radar_pir
        self.threshold_home_activity = threshold_home_activity
        self.alpha = alpha
        self.external_status = []
        self.activity_df = pd.DataFrame()
        self.door_df = pd.DataFrame()

    def parse_data(self):
        activity_data = []
        door_events = []

        for entry in self.raw_data:
            sensor = entry.get("sensor")
            timestamp = entry.get("time")
            try:
                time = pd.to_datetime(timestamp.replace("Z", ""))
            except:
                continue

            if sensor in ["심박", "호흡"]:
                values = entry.get("values", [])
                n = len(values)
                for i, val in enumerate(values):
                    try:
                        activity_data.append({
                            "time": time - timedelta(minutes=(n - 1 - i)),
                            "measurement": sensor,
                            "value": float(val)
                        })
                    except:
                        continue

            elif sensor in ["PIR활동", "레이더활동"]:
                try:
                    activity_data.append({
                        "time": time,
                        "measurement": sensor,
                        "value": float(entry["value"])
                    })
                except:
                    continue

            elif sensor == "문열림":
                door_events.append({"time": time, "event_code": "E0100"})
            elif sensor == "문닫힘":
                door_events.append({"time": time, "event_code": "E0101"})

        # activity_df는 이벤트가 있든 없든 만들어 줌
        self.activity_df = pd.DataFrame(activity_data).sort_values("time")

        # door_events가 비어 있으면 빈 DataFrame만 만들어두고 리턴
        if not door_events:
            self.door_df = pd.DataFrame(columns=["time", "event_code"])
            return

        self.activity_df = pd.DataFrame(activity_data).sort_values("time")
        self.door_df = pd.DataFrame(door_events).sort_values("time")

    def analyze(self):
        self.parse_data()

        if self.door_df.empty:
            print('door 이벤트 없음')
            return

        is_outside = False
        last_door_close_time = None

        i = 0
        while i < len(self.door_df):
            current_event = self.door_df.iloc[i]
            event_time = current_event["time"]

            if current_event["event_code"] == "E0101":
                last_door_close_time = event_time

                if i + 1 < len(self.door_df):
                    next_event = self.door_df.iloc[i + 1]
                    if next_event["event_code"] == "E0100" and (next_event["time"] - event_time) <= timedelta(minutes=3):
                        i += 1
                        continue

                mask_vital = self.activity_df["measurement"].isin(["심박", "호흡"])
                sum_1min = self.activity_df[
                    mask_vital &
                    (self.activity_df["time"] >= event_time + timedelta(minutes=5)) &
                    (self.activity_df["time"] < event_time + timedelta(minutes=30))
                ]["value"].sum()

                is_exit = sum_1min <= self.threshold_heart_breath
                print(f"sum_1min: {sum_1min}, threshold_heart_breath: {self.threshold_heart_breath}")

                if is_exit:
                    mask_motion = self.activity_df["measurement"].isin(["레이더활동", "PIR활동"])
                    sum_30min = self.activity_df[
                        mask_motion &
                        (self.activity_df["time"] >= event_time + timedelta(minutes=30)) &
                        (self.activity_df["time"] < event_time + timedelta(minutes=60))
                    ]["value"].sum()
                    before_30min = self.activity_df[
                        mask_motion &
                        (self.activity_df["time"] >= event_time - timedelta(minutes=30)) &
                        (self.activity_df["time"] < event_time + timedelta(minutes=30))
                    ]["value"].sum()

                    print(f"sum_30min: {sum_30min}, threshold_radar_pir: {self.threshold_radar_pir}")
                    if sum_30min >= self.threshold_radar_pir or before_30min * 0.5 < sum_30min:
                        is_exit = False

                if is_exit and not is_outside:
                    is_outside = True
                    self.external_status.append({"time": event_time, "status": 1})

                    self.threshold_heart_breath = self.alpha * (sum_1min * 1.5) + (1 - self.alpha) * self.threshold_heart_breath
                    self.threshold_radar_pir = self.alpha * (sum_30min * 1.5) + (1 - self.alpha) * self.threshold_radar_pir

            elif current_event["event_code"] == "E0100" and is_outside:
                exit_time = event_time
                mask_motion = self.activity_df["measurement"].isin(["레이더활동", "PIR활동"])
                activity_motion = self.activity_df[mask_motion]

                start_time = last_door_close_time + timedelta(minutes=30)
                while start_time < event_time:
                    end_interval = start_time + timedelta(minutes=30)
                    interval_sum = activity_motion[
                        (activity_motion["time"] >= start_time) & (activity_motion["time"] < end_interval)
                    ]["value"].sum()

                    if interval_sum >= self.threshold_home_activity:
                        exit_time = end_interval
                        self.threshold_home_activity = self.alpha * (interval_sum * 0.7) + (1 - self.alpha) * self.threshold_home_activity
                        break

                    start_time = end_interval

                if exit_time > event_time:
                    exit_time = event_time

                self.external_status.append({"time": exit_time, "status": 0})
                is_outside = False

            i += 1

    def get_results(self):
        if not self.external_status:
            return pd.DataFrame()

        df_status = pd.DataFrame(self.external_status)
        df_status["time"] = pd.to_datetime(df_status["time"])
        df_status["date"] = df_status["time"].dt.date

        outing_periods = []
        current_start = None

        for _, row in df_status.iterrows():
            if row["status"] == 1:
                current_start = row["time"]
            elif row["status"] == 0 and current_start is not None:
                outing_periods.append({
                    "date": current_start.date(),
                    "outing_start": current_start.isoformat(),
                    "outing_end": row["time"].isoformat(),
                    "outing_duration_minutes": (row["time"] - current_start).total_seconds() / 60
                })
                current_start = None

        return pd.DataFrame(outing_periods)
