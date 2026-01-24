Predicted feature: **Produced Energy (kWh)** dtype: float64

### Important: historical data, not predicted.

- **Installed power** - the maximum rate of output of the power plant under specific conditions. Higher installed power generally leads to higher energy production potential.
kWp (kilowatt-peak) is a measure of the peak power output of a photovoltaic system under standard test conditions. It indicates the maximum power the system can produce.
In current model, its important as a scaling factor for energy production. A very important parameter to include, as it directly influences the potential energy output of the solar installation.
- **Vapour Pressure Deficit (VPD)** - the difference between the moisture currently in the air and how much moisture the air can hold when saturated. Basically how dry the air is.
- **Evapotranspiration** - represents how much water is transferred from the soil to the atmosphere. The unit is mm/hour. 1mm of ET = 1 liter of water per square meter per hour.

Wind speed and humidity may affect the cooling of the solar panels. Cooler panels tend to operate more efficiently.

#### Correlations output
```
Produced Energy (kWh)               1.000000
Specific Energy (kWh/kWp)           0.999939
terrestrial_radiation               0.807612
global_tilted_irradiance            0.787937
shortwave_radiation                 0.787937
et0_fao_evapotranspiration          0.754400
global_tilted_irradiance_instant    0.727863
shortwave_radiation_instant         0.725870
direct_radiation                    0.720128
terrestrial_radiation_instant       0.713508
direct_radiation_instant            0.675663
CO2 Avoided (tons)                  0.616015
direct_normal_irradiance            0.581500
diffuse_radiation                   0.577911
vapour_pressure_deficit             0.537914
direct_normal_irradiance_instant    0.497846
diffuse_radiation_instant           0.490449
temperature_2m                      0.486357
apparent_temperature                0.432048
hour                                0.423011
wind_gusts_10m                      0.185344
wind_speed_10m                      0.102641
season                              0.079333
surface_pressure                    0.022527
pressure_msl                        0.013341
dew_point_2m                       -0.003667
year                               -0.006993
angle_of_sun                       -0.008461
day                                -0.017654
Date                               -0.018104
datetime                           -0.018104
month                              -0.046463
cloud_cover_high                   -0.060531
rain                               -0.067428
precipitation                      -0.067428
weather_code                       -0.076468
cloud_cover_mid                    -0.103502
cloud_cover                        -0.134898
cloud_cover_low                    -0.153749
relative_humidity_2m               -0.586769
Name: Produced Energy (kWh), dtype: float64
```

#### Excluded features:
- Produced Energy (kWh) - target
- Specific Energy (kWh/kWp) - derived from target and installed power
- CO2 Avoided (tons) - derived from target
- date, datetime - identifiers
- 
Latitude role is already captured, affects sun heigth, seasonality and day length. This data is already encoded in 
angle_of_sun, month, hour + is_day.
Longitude role is already captured, affects solar noon timing and hourly alignment with sunlight. Data already encoded in hour and angle_of_sun

  
#### Highly collinear

Radiation:
- global_tilted_irradiance
- shortwave_radiation
- direct_radiation
- diffuse_radiation
- direct_normal_irradiance
- global_tilted_irradiance_instant
- shortwave_radiation_instant
- direct_radiation_instant
- diffuse_radiation_instant
- direct_normal_irradiance_instant

We choose only one with the highest correlation: **terrestrial_radiation**.

- relative_humidity_2m
- vapour_pressure_deficit
- et0_fao_evapotranspiration

Pick relative_humidity_2m, highest correlation.

#### Final list of features to include:
- terrestrial_radiation
- temperature_2m
- relative_humidity_2m
- Installed Power (kWp)
- hour
- temperature_2m


