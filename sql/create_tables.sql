START TRANSACTION;

DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS trips;

CREATE TABLE IF NOT EXISTS vehicles (
    vehid NUMERIC PRIMARY KEY,
    vehtype TEXT,
    vehclass TEXT,
    transmission TEXT,
    drive_wheels TEXT,
    gen_weight DOUBLE PRECISION,
    eng_type TEXT,
    eng_dis DOUBLE PRECISION,
    eng_conf TEXT
);

-- SELECT create_distributed_table('vehicles', 'vehid');

CREATE TABLE IF NOT EXISTS trips (
    daynum REAL NOT NULL,
    vehid NUMERIC NOT NULL,
    tripid NUMERIC NOT NULL,
    time NUMERIC NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    long DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION,
    maf DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    abs_load DOUBLE PRECISION,
    oat REAL,
    fuel_rate REAL,
    air_cp_kw REAL,
    air_cp_watts REAL,
    heater_power REAL,
    hv_battery_current REAL,
    hv_battery_soc REAL,
    hv_battery_vol REAL,
    stfb_1 REAL,
    stfb_2 REAL,
    ltfb_1 REAL,
    ltfb_2 REAL
);

-- SELECT create_distributed_table('trips', 'tripid');

ALTER TABLE trips DROP CONSTRAINT IF EXISTS trip_vehicle;
ALTER TABLE trips ADD CONSTRAINT trip_vehicle FOREIGN KEY(vehid) REFERENCES vehicles (vehid);

COMMIT;