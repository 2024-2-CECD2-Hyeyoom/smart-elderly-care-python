import pandas as pd
from datetime import timedelta


class SleepAnalyzer:
    def __init__(self, user_name, sensor_json_data, start_date, end_date):
        self.user_name = user_name
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.sensor_json_data = sensor_json_data

        self.df_all = self.load_json_data()
        self.df_all["date"] = self.df_all["_time"].dt.date

        self.sleep_start_times = {}
        self.wake_start_times = {}
        self.df_sleep_periods = None
        self.df_sleep_daily = None
        self.room_type = None

    def load_json_data(self):
        data = []

        for entry in self.sensor_json_data:
            sensor = entry.get("sensor", "")
            time_str = entry.get("time", "")
            try:
                time = pd.to_datetime(time_str.replace("Z", ""))
            except Exception:
                continue

            values = entry.get("values", [])

            if "심박" in sensor or "호흡" in sensor:
                n = len(values)
                for i, val in enumerate(values):
                    try:
                        minute = time - timedelta(minutes=(n - 1 - i))
                        data.append([
                            "심박" if "심박" in sensor else "호흡",
                            minute,
                            float(val)
                        ])
                    except:
                        continue
            elif "레이더" in sensor or "PIR" in sensor or "조도" in sensor:
                try:
                    val = float(values[0])
                    name = "레이더활동" if "레이더" in sensor else ("PIR활동" if "PIR" in sensor else "조도")
                    data.append([name, time, val])
                except:
                    continue

        df = pd.DataFrame(data, columns=["sensor", "_time", "value"])
        df["_time"] = pd.to_datetime(df["_time"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna()
        df = df.pivot(index="_time", columns="sensor", values="value").reset_index()

        full_index = pd.date_range(start=self.start_date, end=self.end_date, freq="min")
        df_all = pd.DataFrame({"_time": full_index})
        df_all = df_all.merge(df, on="_time", how="left").ffill()

        df_all["조도_threshold"] = df_all.groupby(pd.Grouper(key="_time", freq="12h"))["조도"].transform("min") + 1
        df_all["dark_mask"] = df_all["조도"] <= df_all["조도_threshold"]
        df_all["light_mask"] = ~df_all["dark_mask"]

        df_all["bedroom_candidate"] = (
            df_all["dark_mask"] & (df_all["심박"] > 0) & (df_all["호흡"] > 0) & (df_all["레이더활동"] > 0)
        )
        df_all["living_candidate"] = (
            df_all["dark_mask"] & (df_all["PIR활동"] > 3) &
            (df_all["심박"] == 0) & (df_all["호흡"] == 0) & (df_all["레이더활동"] == 0)
        )

        df_all["date"] = df_all["_time"].dt.date
        return df_all

    def determine_room_type(self):
        if self.df_all["bedroom_candidate"].sum() >= self.df_all["living_candidate"].sum():
            self.room_type = "bedroom"
        else:
            self.room_type = "living_room"

    def detect_sleep_start_times(self):
        for date, group in self.df_all[self.df_all["dark_mask"]].groupby("date"):
            group = group.sort_values("_time").copy()
            group = group[group["_time"].dt.hour >= 20]

            if self.room_type == "bedroom":
                group = group[(group["심박"] > 0) & (group["호흡"] > 0) & (group["레이더활동"] > 0)]
                group["sensor_sum"] = group["심박"] + group["호흡"] + group["레이더활동"]
                group["sensor_drop"] = group["sensor_sum"].shift(1) - group["sensor_sum"]
            else:
                group = group[group["PIR활동"] > 0]
                group["sensor_drop"] = group["PIR활동"].shift(1) - group["PIR활동"]

            if not group.empty:
                idx = group["sensor_drop"].idxmax()
                self.sleep_start_times[date] = group.loc[idx, "_time"]

    def detect_wake_start_times(self):
        for date, group in self.df_all[self.df_all["light_mask"]].groupby("date"):
            group = group.sort_values("_time").copy()
            if self.room_type == "bedroom":
                group["wake_condition"] = (
                    (group["심박"] == 0) & (group["호흡"] == 0) | (group["레이더활동"] == 0)
                ).rolling(window=10, min_periods=1).sum() == 10
            else:
                group["wake_condition"] = (group["PIR활동"] == 0).rolling(window=10, min_periods=1).sum() == 10

            if group["wake_condition"].any():
                self.wake_start_times[date] = group.loc[group["wake_condition"]].iloc[0]["_time"]

    def apply_sleep_state(self):
        df = self.df_all.set_index("_time").copy()
        df["sleep_state"] = 0

        df["awake"] = (
            (df["심박"] == 0) & (df["호흡"] == 0) & (df["레이더활동"] == 0)
            if self.room_type == "bedroom"
            else (df["PIR활동"] == 0)
        )

        awake_threshold = pd.Timedelta(minutes=60)

        for date, sleep_start in self.sleep_start_times.items():
            next_day = date + timedelta(days=1)
            wake_start = self.wake_start_times.get(next_day)
            if sleep_start and wake_start:
                df.loc[(df.index >= sleep_start) & (df.index < wake_start), "sleep_state"] = 1

                segment = df.loc[(df.index >= sleep_start) & (df.index < wake_start)].copy()
                segment["awake_flag"] = segment["awake"]
                segment["group"] = (segment["awake_flag"] != segment["awake_flag"].shift()).cumsum()

                for _, grp_df in segment.groupby("group"):
                    if grp_df["awake_flag"].iloc[0]:
                        duration = grp_df.index[-1] - grp_df.index[0]
                        if duration >= awake_threshold:
                            df.loc[grp_df.index, "sleep_state"] = 0

        self.df_all = df.reset_index()

    def exception_handling(self):
        dark_df = self.df_all[self.df_all["dark_mask"]].copy().sort_values("_time")
        dark_df["time_diff"] = dark_df["_time"].diff()
        dark_df["group"] = (dark_df["time_diff"] != pd.Timedelta(minutes=1)).cumsum()

        for _, group_df in dark_df.groupby("group"):
            total = len(group_df)
            zero_count = ((group_df["심박"] == 0) & (group_df["호흡"] == 0) &
                          (group_df["레이더활동"] == 0) & (group_df["PIR활동"] == 0)).sum()
            if zero_count / total >= 0.9:
                self.df_all.loc[group_df.index, "sleep_state"] = 0

        valid_mask = (self.df_all["dark_mask"]) & (self.df_all["sleep_state"] != 0)
        dark_mask_count = valid_mask.sum()
        zero_sensor_count = (
            ((self.df_all["심박"] == 0) & (self.df_all["호흡"] == 0) &
             (self.df_all["레이더활동"] == 0) & (self.df_all["PIR활동"] == 0)) & valid_mask
        ).sum()

        if dark_mask_count > 0 and zero_sensor_count / dark_mask_count >= 0.6:
            print(f"⚠️ {self.user_name}의 {self.start_date.date()} ~ {self.end_date.date()} 센서 값 60% 이상 0 → 수면 제거")
            self.df_all.loc[valid_mask, "sleep_state"] = 0

    def analyze(self):
        self.determine_room_type()
        self.detect_sleep_start_times()
        self.detect_wake_start_times()
        self.apply_sleep_state()
        self.exception_handling()

    def get_results(self):
        print(f"예상 취침 장소: {self.room_type}")
        df = self.df_all.copy().sort_values("_time")
        df['state_change'] = df['sleep_state'].diff().fillna(0)

        sleep_periods = []
        current_sleep_start = None

        for _, row in df.iterrows():
            if row['state_change'] == 1:
                current_sleep_start = row['_time']
            elif row['state_change'] == -1 and current_sleep_start is not None:
                wake_time = row['_time']
                sleep_periods.append({
                    'date': current_sleep_start.date(),
                    'sleep_start': current_sleep_start,
                    'wake_time': wake_time,
                    'sleep_duration': wake_time - current_sleep_start
                })
                current_sleep_start = None

        df_sleep_periods = pd.DataFrame(sleep_periods)
        self.df_sleep_periods = df_sleep_periods

        # 비어있을 경우
        if df_sleep_periods.empty:
            print(f"⚠️ {self.user_name}의 수면 구간 데이터가 없습니다.")
            self.df_sleep_daily = pd.DataFrame(columns=["date", "total_sleep_duration"])
            return self.room_type, self.df_sleep_periods, self.df_sleep_daily

        # 비어있지 않은 경우에만 계산
        df_sleep_periods['adjusted_wake_date'] = df_sleep_periods['wake_time'].apply(
            lambda x: x.date() if x.hour < 12 else x.date() + timedelta(days=1)
        )
        self.df_sleep_daily = (
            df_sleep_periods.groupby('adjusted_wake_date')['sleep_duration']
            .sum()
            .reset_index()
            .rename(columns={'adjusted_wake_date': 'date', 'sleep_duration': 'total_sleep_duration'})
        )

        return self.room_type, self.df_sleep_periods, self.df_sleep_daily
