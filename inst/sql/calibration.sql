/* Instrument calibration */

create table autocals (
  measurement_type_id int references measurement_types,
  type text,
  dates tsrange,
  times timerange,
  value numeric default 0,
  primary key(measurement_type_id, dates, times),
  CONSTRAINT no_overlapping_autocals EXCLUDE USING GIST (
    measurement_type_id WITH =,
    dates WITH &&,
    times WITH &&
  )
);

create table manual_calibrations (
  file_id int references files on delete cascade,
  measurement_type_id int references measurement_types,
  type text,
  times tsrange,
  provided_value numeric,
  measured_value numeric,
  corrected boolean not null,
  primary key(measurement_type_id, type, times),
  CONSTRAINT no_overlapping_manual_cals EXCLUDE USING GIST (
    measurement_type_id WITH =,
    type WITH =,
    times WITH &&
  )
);

create table gilibrator (
  site_id int references sites not null,
  measurement_name text not null,
  time timestamp,
  certified_value numeric,
  measured_value numeric,
  changed boolean,
  primary key(site_id, measurement_name, time)
);

create table calibration_flags (
  measurement_type_id int references measurement_types,
  type text,
  times tsrange,
  explanation text,
  primary key(measurement_type_id, type, times)
);

-- calibration results calculated in load_calibration.R
create table calibrations_wfms (
  measurement_type_id int not null,
  type text not null,
  time timestamp not null,
  provided_value numeric,
  measured_value numeric,
  flagged bool,
  unique(measurement_type_id, type, time)
);
create table calibrations_wfml (
  measurement_type_id int not null,
  type text not null,
  time timestamp not null,
  provided_value numeric,
  measured_value numeric,
  flagged bool,
  unique(measurement_type_id, type, time)
);
create table calibrations_psp (
  measurement_type_id int not null,
  type text not null,
  time timestamp not null,
  provided_value numeric,
  measured_value numeric,
  flagged bool,
  unique(measurement_type_id, type, time)
);
create view calibration_results as
  select * from calibrations_wfms
   union
  select * from calibrations_wfml
   union
  select * from calibrations_psp;
