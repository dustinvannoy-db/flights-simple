from dataclasses import dataclass

@dataclass
class DelayType():
    WeatherDelay: str = "NA"
    NASDelay: str = "NA"
    SecurityDelay: str = "NA"
    LateAircraftDelay: str = "NA"
    IsArrDelayed: str = "NO"
    IsDepDelayed: str = "NO"
    delay_type: str = "UncategorizedDelay"


delay_type_usecases = [
    DelayType(WeatherDelay="O", delay_type="WeatherDelay"),
    DelayType(NASDelay="0", delay_type="NASDelay"),
    DelayType(SecurityDelay="0", delay_type="SecurityDelay"),
    DelayType(LateAircraftDelay="0", delay_type='LateAircraftDelay'),
    DelayType(IsArrDelayed="YES", delay_type="UncategorizedDelay"),
    DelayType(IsDepDelayed="YES", delay_type="UncategorizedDelay"),
    DelayType(
        WeatherDelay='0', NASDelay="0", SecurityDelay="0", LateAircraftDelay="0", IsArrDelayed="YES",
        IsDepDelayed="YES", delay_type="WeatherDelay"
    )
]