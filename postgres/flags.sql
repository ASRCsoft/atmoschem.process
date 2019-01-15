/* Determing flag values. */

create table manual_flags (
  measurement text,
  station_id int references stations not null,
  source sourcerange not null,
  explanation text,
  CONSTRAINT overlapping_sources EXCLUDE USING GIST (
    measurement WITH =,
    station_id WITH =,
    source WITH &&
  )
);

create or replace function has_manual_flag(measurement text, station_id int, source sourcerow) RETURNS bool AS $$
  select exists();
$$ LANGUAGE sql;

/* Determine if a measurement is flagged. */
create or replace function is_flagged(measurement text, station_id int, source sourcerow, value numeric, flag text, median numeric, mad numeric) RETURNS bool AS $$
  begin
    -- steps:
    -- 1) apply manual flags
    -- 2) instrument flags
    -- 3) calibration flags
    -- 4) extreme value flags
    -- 5) outlier flags (based on the Hampel filter)
    if (value - median) / mad > 3 then
      return true;
    end if;
    -- 6) no flag
    return false;
  end;
$$ LANGUAGE plpgsql;
